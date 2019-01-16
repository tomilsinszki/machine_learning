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

    protected function configure() {
        $this->setName('rabattcorner:pending:transactions');
        $this->setDescription('Pending Transaction List');
    }

    private function loadMonthlyPeriods() {
        $this->periods = array();

        $now = new \DateTime();
        $currentDay = new \DateTime();
        $currentDay->setDate(2017, 1, 1);
        $currentDay->setTime(0, 0, 0, 0);

        $previousYear = 2016;
        $previousMonth = 12;

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
            if ($now < $currentDay) {
                $shouldRun = false;
            }
        }

        array_pop($this->periods);
    }

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

    /**
     * @throws DBALException
     */
    private function loadUserIds() {
        $this->userIds = array();

        $statement = $this->entityManager->getConnection()->prepare("SELECT id FROM account_user WHERE account_user.id AND account_user.locked = 0 AND account_user.enabled=1 ORDER BY id");
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

        $this->loadMonthlyPeriods();
        $this->loadUserIds();

        foreach ($this->userIds as $userId) {
            $firstAndLastContactIdStatement = $this->entityManager->getConnection()->prepare("SELECT MIN(contact_id) AS `first`, MAX(contact_id) AS `last` FROM user_contact WHERE user_id={$userId}");
            $firstAndLastContactIdStatement->execute();
            $firstAndLastContactResult = $firstAndLastContactIdStatement->fetchAll();

            $firstContactId = $firstAndLastContactResult[0]['first'];
            $lastContactId = $firstAndLastContactResult[0]['last'];

            $signupDateStatement = $this->entityManager->getConnection()->prepare("SELECT `time` FROM Contact WHERE id={$firstContactId}");
            $signupDateStatement->execute();
            $signupDateString = $signupDateStatement->fetchColumn(0);
            $signupDateString = substr($signupDateString, 0, 10);

            $contactDetailsStatement = $this->entityManager->getConnection()->prepare("SELECT `title`, `postalcode`, `language`, `birthday` FROM Contact WHERE id={$lastContactId}");
            $contactDetailsStatement->execute();
            $contactDetailsResult = $contactDetailsStatement->fetchAll();
            $postalCode = $contactDetailsResult[0]['postalcode'];
            $gender = $contactDetailsResult[0]['title'];
            $language = $contactDetailsResult[0]['language'];
            $birthday = $contactDetailsResult[0]['birthday'];
            $birthday = substr($birthday, 0, 10);

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

            $partnersStatement = $this->entityManager->getConnection()->prepare("SELECT id FROM Partner WHERE status='active'");
            $partnersStatement->execute();
            $partnerIdResults = $partnersStatement->fetchAll();

            foreach ($partnerIdResults as $partnerIdResult) {
                $partnerId = intval($partnerIdResult['id']);

                foreach ($this->periods as $periodName => $period) {
                    /** @var \DateTime $start */
                    $start = $period['start'];

                    /** @var \DateTime $end */
                    $end = $period['end'];

                    $startString = "{$start->format('Y-m-d H:i:s')}";
                    $endString = "{$end->format('Y-m-d H:i:s')}";

                    $signupDateTime = new \DateTime("{$signupDateString} 00:00:00");

                    if ($end->getTimestamp() <= $signupDateTime->getTimestamp()) {
                        continue;
                    }

                    $transactionStatement = $this->entityManager->getConnection()->prepare("SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId} AND '{$startString}'<=time AND time<='{$endString}'");
                    $transactionStatement->execute();
                    $transactionProgramAmountResults = $transactionStatement->fetchAll();
                    $transactionSumProgramAmount = empty($transactionProgramAmountResults[0]['program_amount']) ? 0.0 : $transactionProgramAmountResults[0]['program_amount'];
                    $transactionSumProgramAmount = number_format($transactionSumProgramAmount, '2', '.', '');

                    $previousVisitCountStatement = $this->entityManager->getConnection()->prepare("SELECT COUNT(DISTINCT id) AS visit_count FROM PartnerVisit WHERE user_id={$userId} AND partner_id={$partnerId} AND time<\"{$start->format('Y-m-d')}\"");
                    $previousVisitCountStatement->execute();
                    $previousVisitCountResults = $previousVisitCountStatement->fetchAll();
                    $previousVisitCount = empty($previousVisitCountResults[0]['visit_count']) ? 0 : intval($previousVisitCountResults[0]['visit_count']);

                    //echo("userId,{$userId},partnerId,{$partnerId},year,{$year},week,{$weekNumber},start,{$start->format('Y-m-d')},end,{$end->format('Y-m-d')},sum,{$transactionSumProgramAmount},signup,{$signupDateString},postalcode,{$postalCode},gender,{$gender}\n");

                    echo("{$userId},{$signupDateString},{$postalCode},{$gender},{$language},{$birthday},{$partnerId},{$start->format('Y-m-d')},{$end->format('Y-m-d')},{$previousVisitCount},{$transactionSumProgramAmount}\n");
                }
            }
        }
    }
}