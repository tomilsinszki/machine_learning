<?php

namespace RabattCorner\CashbackBundle\Command;

ini_set('memory_limit', '1524M');
ini_set('max_input_time', -1);
ini_set('max_execution_time', 0);
set_time_limit(0);
ini_set('display_errors',1);
error_reporting(E_ALL|E_STRICT);

use Doctrine\ORM\EntityManager;
use RabattCorner\AccountBundle\Entity\UserRepository;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Doctrine\DBAL\DBALException;

class PendingTransactionsReportCommand extends ContainerAwareCommand
{
    /**
     * @var EntityManager
     */
    private $entityManager;

    /**
     * @var array
     */
    private $periods = array();

    /**
     * @var array
     */
    private $userIds = array();

    /**
     * @var string
     */
    private $startYear = null;

    /**
     * @var string
     */
    private $startMonth = null;

    /**
     * @var string
     */
    private $startPreviousYear = null;

    /**
     * @var string
     */
    private $startPreviousMonth = null;

    /**
     * @var string
     */
    private $endYear = null;

    /**
     * @var string
     */
    private $endMonth = null;

    /**
     * @var int
     */
    private $partnerBufferMonthCount = 0;

    /**
     * @var int
     */
    private $userBufferMonthCount = 0;

    /**
     * @var int
     */
    private $userMinimumAcceptedTransactionAmount = 0;

    protected function configure() {
        $this->setName('rabattcorner:pending:transactions');
        $this->setDescription('Pending Transaction List');

        $this->addArgument(
            'startYear',
            InputArgument::REQUIRED,
            "The year of the first period to generate data for"
        );

        $this->addArgument(
            'startMonth',
            InputArgument::REQUIRED,
            "The month of the first period to generate data for"
        );

        $this->addArgument(
            'startPreviousYear',
            InputArgument::REQUIRED,
            "The year of the period that would be before the first period"
        );

        $this->addArgument(
            'startPreviousMonth',
            InputArgument::REQUIRED,
            "The month of the period that would be before the first period"
        );

        $this->addArgument(
            'endYear',
            InputArgument::REQUIRED,
            "The year of the last period to generate data for"
        );

        $this->addArgument(
            'endMonth',
            InputArgument::REQUIRED,
            "The month of the last period to generate data for"
        );

        $this->addArgument(
            'partnerBufferMonthCount',
            InputArgument::REQUIRED,
            "The number of months the partner should have existed already (before the first day of the current month)"
        );

        $this->addArgument(
            'userBufferMonthCount',
            InputArgument::REQUIRED,
            "User should have accepted transactions this many months ago"
        );

        $this->addArgument(
            'userMinimumAcceptedTransactionAmount',
            InputArgument::REQUIRED,
            "Filter out users who's sum accepted transaction amount is less than this amount"
        );
    }

    private function loadMonthlyPeriods() {
        $this->periods = array();

        $endDate = new \DateTime();
        $endDate->setDate($this->endYear, $this->endMonth, 1);
        $endDate->setTime(0, 0, 0);

        $currentDay = new \DateTime();
        $currentDay->setDate($this->startYear, $this->startMonth, 1);
        $currentDay->setTime(0, 0, 0, 0);

        $previousYear = $this->startPreviousYear;
        $previousMonth = $this->startPreviousMonth;

        $shouldRun = true;
        while ($shouldRun) {
            $year = $currentDay->format("Y");
            $month = $currentDay->format("n");

            $periodKey = "{$year}-{$month}";
            $previousPeriodKey = "{$previousYear}-{$previousMonth}";

            $isNewPeriod = !isset($this->periods[$periodKey]);
            $hasPreviousPeriod = isset($this->periods[$previousPeriodKey]['start']);

            if ($isNewPeriod) {
                $this->periods[$periodKey] = ['start' => clone $currentDay];

                if ($hasPreviousPeriod) {
                    $this->periods[$previousPeriodKey]['end'] = clone $currentDay;
                }

                $previousYear = $year;
                $previousMonth = $month;
            }

            $currentDay->modify('+1 day');
            if ($endDate < $currentDay) {
                $shouldRun = false;
            }
        }

        array_pop($this->periods);
    }

    // TODO: review before enabling again
    /*
    private function loadWeeklyPeriods() {
        $this->periods = array();

        $now = new \DateTime();
        $currentDay = new \DateTime();
        $currentDay->setDate(2017, 1, 2);
        $currentDay->setTime(0, 0, 0, 0);

        $previousYear = 2016;
        $previousWeekNumber = 52;

        $shouldRun = true;
        while ($shouldRun) {
            $year = $currentDay->format("Y");
            $weekNumber = ltrim($currentDay->format("W"), '0');

            $periodKey = "{$year}-{$weekNumber}";
            $previousPeriodKey = "{$previousYear}-{$previousWeekNumber}";

            $isNewPeriod = !isset($this->periods[$periodKey]);
            $hasPreviousPeriod = isset($this->periods[$previousPeriodKey]['start']);

            if ($isNewPeriod) {
                $this->periods[$periodKey] = ['start' => clone $currentDay];

                if ($hasPreviousPeriod) {
                    $this->periods[$previousPeriodKey]['end'] = clone $currentDay;
                }

                $previousYear = $year;
                $previousWeekNumber = $weekNumber;
            }

            $currentDay->modify('+1 day');
            if ($now < $currentDay) {
                $shouldRun = false;
            }
        }

        array_pop($this->periods);
    }
    */

    /**
     * @throws DBALException
     */
    private function loadUserIds() {
        $this->userIds = array();

        $statement = $this->entityManager->getConnection()->prepare("SELECT u.id, SUM(programAmount) sum_program_amount FROM account_user u LEFT JOIN cashback_transaction t ON u.id=t.user_id WHERE u.locked=0 AND u.enabled=1 AND t.`time`<DATE_FORMAT(CURDATE(), '%Y-%m-01') - INTERVAL {$this->userBufferMonthCount} MONTH AND t.status='accepted' GROUP BY u.id HAVING {$this->userMinimumAcceptedTransactionAmount}<sum_program_amount ORDER BY u.id ASC");
        $statement->execute();
        $userIdResults = $statement->fetchAll();

        foreach($userIdResults as $userIdResult) {
            $this->userIds[] = intval($userIdResult['id']);
        }
    }

    /**
     * @param InputInterface $input
     * @param OutputInterface $output
     * @return int|null|void
     * @throws DBALException
     */
    protected function execute(InputInterface $input, OutputInterface $output) {
        $this->entityManager = $this->getContainer()->get('doctrine')->getManager();

        $this->startYear = $input->getArgument('startYear');
        $this->startMonth = $input->getArgument('startMonth');

        $this->startPreviousYear = $input->getArgument('startPreviousYear');
        $this->startPreviousMonth = $input->getArgument('startPreviousMonth');

        $this->endYear = $input->getArgument('endYear');
        $this->endMonth = $input->getArgument('endMonth');

        $this->partnerBufferMonthCount = $input->getArgument('partnerBufferMonthCount');
        $this->userBufferMonthCount = $input->getArgument('userBufferMonthCount');
        $this->userMinimumAcceptedTransactionAmount = $input->getArgument('userMinimumAcceptedTransactionAmount');

        $connection = $this->entityManager->getConnection();
        $connection->getConfiguration()->setSQLLogger(null);

        $this->loadMonthlyPeriods();
        $this->loadUserIds();

        foreach ($this->periods as $periodName => $period) {
            foreach ($this->userIds as $userId) {
                $firstAndLastContactIdStatement = $connection->prepare("SELECT MIN(contact_id) AS `first`, MAX(contact_id) AS `last` FROM user_contact WHERE user_id={$userId}");
                $firstAndLastContactIdStatement->execute();
                $firstAndLastContactResult = $firstAndLastContactIdStatement->fetchAll();

                $firstContactId = $firstAndLastContactResult[0]['first'];
                $lastContactId = $firstAndLastContactResult[0]['last'];

                $signupDateStatement = $connection->prepare("SELECT `time` FROM Contact WHERE id={$firstContactId}");
                $signupDateStatement->execute();
                $signupDateString = $signupDateStatement->fetchColumn(0);
                $signupDateString = substr($signupDateString, 0, 10);

                $contactDetailsStatement = $connection->prepare("SELECT `title`, `postalcode`, `language`, `birthday` FROM Contact WHERE id={$lastContactId}");
                $contactDetailsStatement->execute();
                $contactDetailsResult = $contactDetailsStatement->fetchAll();

                $rawPostalCode = $contactDetailsResult[0]['postalcode'];
                $postalCodeDigits0 = null;
                $postalCodeDigits1 = null;
                $postalCodeDigits2 = null;
                $postalCodeDigits3 = null;

                if (is_numeric($rawPostalCode)) {
                    if ( (999 < intval($rawPostalCode)) and (intval($rawPostalCode) < 9700)) {
                        $postalCodeDigits0 = substr($rawPostalCode, 0, 1);
                        $postalCodeDigits1 = substr($rawPostalCode, 1, 1);
                        $postalCodeDigits2 = substr($rawPostalCode, 2, 1);
                        $postalCodeDigits3 = substr($rawPostalCode, 3, 1);
                    }
                }

                $gender = $contactDetailsResult[0]['title'];
                $language = $contactDetailsResult[0]['language'];
                $birthday = $contactDetailsResult[0]['birthday'];
                $birthday = substr($birthday, 0, 4);

                if ($gender === 'male') {
                    $gender = '1';
                } elseif ($gender === 'female') {
                    $gender = '2';
                } else {
                    $gender = '';
                }

                if ($language === 'de') {
                    $language = '1';
                } elseif ($language === 'fr') {
                    $language = '2';
                }

                $partnersStatement = $connection->prepare("SELECT p.id, SUM(t.programAmount) sum_program_amount FROM Partner p LEFT JOIN cashback_transaction t ON p.id=t.partner_id WHERE t.`time`<DATE_FORMAT(CURDATE(), '%Y-%m-01') - INTERVAL {$this->partnerBufferMonthCount} MONTH AND t.`status`='accepted' AND p.`status`='active' GROUP BY p.id HAVING 0<sum_program_amount");
                $partnersStatement->execute();
                $partnerIdResults = $partnersStatement->fetchAll();

                foreach ($partnerIdResults as $partnerIdResult) {
                    /** @var \DateTime $start */
                    $start = $period['start'];

                    /** @var \DateTime $end */
                    $end = $period['end'];

                    $startString = "{$start->format('Y-m-d H:i:s')}";
                    $endString = "{$end->format('Y-m-d H:i:s')}";

                    $signupDateTime = new \DateTime("{$signupDateString} 00:00:00");

                    /*
                    if ($end->getTimestamp() <= $signupDateTime->getTimestamp()) {
                        continue;
                    }
                    */

                    $partnerId = intval($partnerIdResult['id']);

                    $transactionStatement = $connection->prepare("SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId} AND '{$startString}'<=time AND time<='{$endString}'");
                    $transactionStatement->execute();
                    $transactionProgramAmountResults = $transactionStatement->fetchAll();
                    $transactionSumProgramAmount = empty($transactionProgramAmountResults[0]['program_amount']) ? 0.0 : $transactionProgramAmountResults[0]['program_amount'];
                    $transactionSumProgramAmount = number_format($transactionSumProgramAmount, '2', '.', '');
                    if ($transactionSumProgramAmount === '0.00') {
                        $transactionSumProgramAmount = '0.001';
                    }

                    $previousVisitCountStatement = $connection->prepare("SELECT COUNT(DISTINCT id) AS visit_count FROM PartnerVisit WHERE user_id={$userId} AND partner_id={$partnerId} AND time<\"{$start->format('Y-m-d')}\"");
                    $previousVisitCountStatement->execute();
                    $previousVisitCountResults = $previousVisitCountStatement->fetchAll();
                    $previousVisitCount = empty($previousVisitCountResults[0]['visit_count']) ? 0 : intval($previousVisitCountResults[0]['visit_count']);

                    echo("{$start->format('Y')},{$start->format('n')},{$userId},{$signupDateTime->format('Y')},{$signupDateTime->format('n')},{$postalCodeDigits0},{$postalCodeDigits1},{$postalCodeDigits2},{$postalCodeDigits3},{$gender},{$language},{$birthday},{$partnerId},{$previousVisitCount},{$transactionSumProgramAmount}\n");
                }
            }
        }
    }
}