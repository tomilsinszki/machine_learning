<?php

namespace RabattCorner\CashbackBundle\Command;

ini_set('memory_limit', '1524M');
ini_set('max_input_time', -1);
ini_set('max_execution_time', 0);
set_time_limit(0);
ini_set('display_errors',1);
error_reporting(E_ALL|E_STRICT);

use Doctrine\ORM\EntityManager;
use Symfony\Bundle\FrameworkBundle\Command\ContainerAwareCommand;
use Symfony\Component\Console\Input\InputArgument;
use Symfony\Component\Console\Input\InputInterface;
use Symfony\Component\Console\Output\OutputInterface;
use Doctrine\DBAL\DBALException;
use Symfony\Component\Validator\Constraints\DateTime;
use Doctrine\DBAL\Connection;

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

                $partnersStatement = $connection->prepare("SELECT p.id, p.main_category_id as category_id, c.parent_id as parent_category_id, SUM(t.programAmount) sum_program_amount FROM Partner p LEFT JOIN cashback_transaction t ON p.id=t.partner_id LEFT JOIN Category c ON p.main_category_id=c.id WHERE t.`time`<DATE_FORMAT(CURDATE(), '%Y-%m-01') - INTERVAL {$this->partnerBufferMonthCount} MONTH AND t.`status`='accepted' AND p.`status`='active' GROUP BY p.id HAVING 0<sum_program_amount");
                $partnersStatement->execute();
                $partnerIdResults = $partnersStatement->fetchAll();

                foreach ($partnerIdResults as $partnerIdResult) {
                    /** @var \DateTime $start */
                    $start = $period['start'];

                    /** @var \DateTime $end */
                    $end = $period['end'];

                    $startString = "{$start->format('Y-m-d H:i:s')}";
                    $endString = "{$end->format('Y-m-d H:i:s')}";

                    $age = 0;
                    if (!empty($birthday)) {
                        $age = $start->format('Y') - $birthday;
                    }

                    $mainCategoryId = intval($partnerIdResult['parent_category_id']);
                    if (empty($mainCategoryId)) {
                        $mainCategoryId = intval($partnerIdResult['category_id']);
                    }

                    $signupDateTime = new \DateTime("{$signupDateString} 00:00:00");

                    $signupTimestamp = strtotime($signupDateTime->format('Y-m-d H:i:s'));
                    $startTimestamp = strtotime($start->format('Y-m-d H:i:s'));

                    $secondsSinceSignup = 0;
                    if ($signupTimestamp < $startTimestamp) {
                        $secondsSinceSignup = $startTimestamp - $signupTimestamp;
                    }

                    $monthsSinceSignup = floor(($secondsSinceSignup / (60 * 60 * 24 * 30)));
                    $monthsSinceSignup = intval($monthsSinceSignup);

                    /*
                    if ($end->getTimestamp() <= $signupDateTime->getTimestamp()) {
                        continue;
                    }
                    */

                    $partnerId = intval($partnerIdResult['id']);

                    $dates = array();

                    $tmpStart = clone $start;
                    $tmpStart->modify('-1 month');
                    $tmpEnd = clone $start;
                    $dates[0] = array('start' => $tmpStart, 'end' => $tmpEnd);

                    $tmpStart = clone $start;
                    $tmpStart->modify('-4 month');
                    $tmpEnd = clone $start;
                    $tmpEnd->modify('-1 month');
                    $dates[1] = array('start' => $tmpStart, 'end' => $tmpEnd);

                    $tmpStart = clone $start;
                    $tmpStart->modify('-12 month');
                    $tmpEnd = clone $start;
                    $tmpEnd->modify('-11 month');
                    $dates[2] = array('start' => $tmpStart, 'end' => $tmpEnd);

                    $tmpStart = clone $start;
                    $tmpStart->modify('-15 month');
                    $tmpEnd = clone $start;
                    $tmpEnd->modify('-12 month');
                    $dates[3] = array('start' => $tmpStart, 'end' => $tmpEnd);

                    $tmpEnd = clone $start;
                    $dates[4] = array('start' => null, 'end' => $tmpEnd);

                    $export = array();

                    foreach ($dates as $k2 => $date) {
                        foreach (array(0 => array($partnerId), 1 => $this->getRecommendedPartnerIds($connection, $partnerId), 3 => null) as $k3 => $currentPartnerIds) {
                            $export[0][$k3][$k2] = $this->zScoreVisitCount($connection, $currentPartnerIds, $userId, $date['start'], $date['end']);
                            $export[1][$k3][$k2] = $this->zScoreProgramAmount($connection, $currentPartnerIds, $userId, $date['start'], $date['end']);
                            $export[2][$k3][$k2] = $this->zScoreTransactionCount($connection, $currentPartnerIds, $userId, $date['start'], $date['end']);

                            if ($k3 !== 0) {
                                $export[3][$k3][$k2] = $this->zScoreNumberOfPartnersVisited($connection, $currentPartnerIds, $userId, $date['start'], $date['end']);
                                $export[4][$k3][$k2] = $this->zScoreNumberOfPartnersSpentAt($connection, $currentPartnerIds, $userId, $date['start'], $date['end']);
                            }
                        }

                        $export[0][2][$k2] = $this->zScoreVisitCountForMainCategory($connection, $mainCategoryId, $userId, $date['start'], $date['end']);
                        $export[1][2][$k2] = $this->zScoreProgramAmountForMainCategory($connection, $mainCategoryId, $userId, $date['start'], $date['end']);
                        $export[2][2][$k2] = $this->zScoreTransactionCountForMainCategory($connection, $mainCategoryId, $userId, $date['start'], $date['end']);
                        $export[3][2][$k2] = $this->zScoreNumberOfPartnersVisitedForMainCategory($connection, $mainCategoryId, $userId, $date['start'], $date['end']);

                        ksort($export[0]);
                        ksort($export[1]);
                        ksort($export[2]);
                        ksort($export[3]);
                        ksort($export[4]);
                    }

                    print_r($export);
                    exit();

                    // xyz
                    /*
                    foreach ($dates as $k2 => $date) {
                        foreach (array(null, $userId) as $k4 => $currentUserId) {
                            foreach (array(null, array($partnerId)) as $k3 => $currentPartnerIds) {
                                $export[0][$k2][$k3][$k4] = $this->generalGetVisitCount($connection, $currentPartnerIds, $currentUserId, $date['start'], $date['end']);
                                $export[1][$k2][$k3][$k4] = $this->generalGetSumProgramAmount($connection, $currentPartnerIds, $currentUserId, $date['start'], $date['end']);
                            }
                            $export[0][$k2][3][$k4] = $this->getVisitCountForMainCategory($connection, $mainCategoryId, $currentUserId, $date['start'], $date['end']);
                            $export[1][$k2][3][$k4] = $this->getSumProgramAmountForMainCategory($connection, $mainCategoryId, $currentUserId, $date['start'], $date['end']);
                        }
                    }
                    */

                    /*
                    $transactionStatement = $connection->prepare("SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId} AND '{$startString}'<=time AND time<='{$endString}'");
                    $transactionStatement->execute();
                    $transactionProgramAmountResults = $transactionStatement->fetchAll();
                    $transactionSumProgramAmount = empty($transactionProgramAmountResults[0]['program_amount']) ? 0.0 : $transactionProgramAmountResults[0]['program_amount'];
                    $transactionSumProgramAmount = floatval($transactionSumProgramAmount);
                    $transactionSumProgramAmount = $this->calculateTarget($transactionSumProgramAmount);

                    $output = "";

                    $output .= "{$start->format('Y')}";
                    $output .= ",";
                    $output .= "{$start->format('n')}";
                    $output .= ",";
                    $output .= "{$userId}";
                    $output .= ",";
                    $output .= "{$monthsSinceSignup}";
                    $output .= ",";
                    $output .= "{$postalCodeDigits0}";
                    $output .= ",";
                    $output .= "{$postalCodeDigits1}";
                    $output .= ",";
                    $output .= "{$postalCodeDigits2}";
                    $output .= ",";
                    $output .= "{$postalCodeDigits3}";
                    $output .= ",";
                    $output .= "{$gender}";
                    $output .= ",";
                    $output .= "{$language}";
                    $output .= ",";
                    $output .= "{$age}";
                    $output .= ",";
                    $output .= "{$partnerId}";
                    $output .= ",";
                    $output .= "{$mainCategoryId}";
                    $output .= ",";
                    $output .= "{$countRecommendedPartnerIds}";
                    $output .= ",";

                    echo($output);
                    */
                }
            }
        }
    }

    /**
     * @param Connection $connection
     * @param $userId
     * @param $start
     * @param $end
     * @return int
     * @throws DBALException
     */
    private function getNumberOfPartnersVisited(Connection $connection, $userId, $start, $end) {
        $queryText = "SELECT COUNT(DISTINCT partner_id) AS visit_count FROM PartnerVisit WHERE user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $count = empty($results[0]['visit_count']) ? 0 : $results[0]['visit_count'];
        return $count;
    }

    private function getNumberOfPartnersSpentInMainCategory(Connection $connection, $mainCategoryId, $userId, $start, $end) {
        $queryText = "SELECT COUNT(DISTINCT partner_id) AS partner_count FROM cashback_transaction WHERE user_id={$userId}";

        // TODO: implement
    }

    /**
     * @param Connection $connection
     * @param $userId
     * @param $start
     * @param $end
     * @return int
     * @throws DBALException
     */
    private function getNumberOfPartnersSpent(Connection $connection, $userId, $start, $end) {
        $queryText = "SELECT COUNT(DISTINCT partner_id) AS partner_count FROM cashback_transaction WHERE user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $count = empty($results[0]['partner_count']) ? 0 : $results[0]['partner_count'];
        return $count;
    }

    /**
     * @param Connection $connection
     * @param $partnerId
     * @return array
     * @throws DBALException
     */
    private function getRecommendedPartnerIds(Connection $connection, $partnerId) {
        $queryText = "SELECT p2.id AS recommended_partner_id FROM Partner p1 LEFT JOIN recommended_partners rp ON p1.id=rp.partner_id LEFT JOIN Partner p2 ON rp.recommended_partner_id=p2.id WHERE p1.id={$partnerId}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $recommendedPartnerIds = array();

        foreach ($results as $result) {
            $recommendedPartnerId = $result['recommended_partner_id'];
            $recommendedPartnerIds[] = $recommendedPartnerId;
        }

        $recommendedPartnerIds = array_unique($recommendedPartnerIds);

        return $recommendedPartnerIds;
    }

    /**
     * @param Connection $connection
     * @param $partnerId
     * @param $userId
     * @param $start
     * @param $end
     * @return float|string
     * @throws DBALException
     */
    private function getSumProgramAmountForRecommendedPartners(Connection $connection, $partnerId, $userId, $start, $end) {
        $recommendedPartnerIds = $this->getRecommendedPartnerIds($connection, $partnerId);
        $recommendedPartnerIdsList = implode(',', $recommendedPartnerIds);

        if (empty($recommendedPartnerIdsList)) {
            return 0;
        }

        $queryText = "SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id IN ({$recommendedPartnerIdsList}) AND user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $amount = empty($results[0]['program_amount']) ? 0.0 : $results[0]['program_amount'];

        return $amount;
    }

    /**
     * @param Connection $connection
     * @param $partnerId
     * @param $userId
     * @param $start
     * @param $end
     * @return int
     * @throws DBALException
     */
    private function getVisitCountForRecommendedPartners(Connection $connection, $partnerId, $userId, $start, $end) {
        $recommendedPartnerIds = $this->getRecommendedPartnerIds($connection, $partnerId);
        $recommendedPartnerIdsList = implode(',', $recommendedPartnerIds);

        if (empty($recommendedPartnerIdsList)) {
            return 0;
        }

        $queryText = "SELECT COUNT(DISTINCT PartnerVisit.id) AS visit_count FROM PartnerVisit WHERE partner_id IN ({$recommendedPartnerIdsList}) AND user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $visitCount = empty($results[0]['visit_count']) ? 0 : intval($results[0]['visit_count']);

        return $visitCount;
    }

    /*
    private function getSubCategoryIds(Connection $connection, $partnerId) {
        $queryText = "SELECT main_category_id FROM Partner p LEFT JOIN Category c ON p.main_category_id=c.id WHERE p.id={$partnerId} AND c.parent_id IS NOT NULL";

        $subCategoryIds = array();

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $subCategoryId = empty($results[0]['main_category_id']) ? 0 : $results[0]['main_category_id'];

        if (!empty($subCategoryId)) {
            $subCategoryIds[] = $subCategoryId;
        }

        $queryText = "SELECT category_id FROM partner_category pc LEFT JOIN Category c ON pc.category_id=c.id WHERE pc.partner_id={$partnerId} AND c.parent_id IS NOT NULL";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        foreach ($results as $result) {
            $subCategoryId = $result['category_id'];
            $subCategoryIds[] = $subCategoryId;
        }

        $subCategoryIds = array_unique($subCategoryIds);

        return $subCategoryIds;
    }
    */

    /*
    private function getSumProgramAmountForSubCategories(Connection $connection, $partnerId, $userId, $start, $end) {
        $subCategoryIds = $this->getSubCategoryIds($connection, $partnerId);
        $subCategoryIdsList = implode(',', $subCategoryIds);

        if (empty($subCategoryIdsList)) {
            return 0.001;
        }

        $queryText = "SELECT SUM(programAmount) AS program_amount FROM cashback_transaction LEFT JOIN Partner ON cashback_transaction.partner_id=Partner.id LEFT JOIN partner_category ON Partner.id=partner_category.partner_id WHERE partner_category.category_id IN ({$subCategoryIdsList}) AND user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $amount = empty($results[0]['program_amount']) ? 0.0 : $results[0]['program_amount'];
        $amount = number_format($amount, '2', '.', '');
        if ($amount === '0.00') {
            $amount = '0.001';
        }

        return $amount;
    }
    */

    /**
     * @param Connection $connection
     * @param $mainCategoryId
     * @param $userId
     * @param $start
     * @param $end
     * @return float|string
     * @throws DBALException
     */
    private function getSumProgramAmountForMainCategory(Connection $connection, $mainCategoryId, $userId, $start, $end) {
        $queryText = "SELECT SUM(t.programAmount) AS program_amount FROM cashback_transaction t LEFT JOIN Partner p ON t.partner_id=p.id LEFT JOIN Category c ON p.main_category_id=c.id LEFT JOIN Category cp ON c.parent_id=cp.id WHERE p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))";

        if (!empty($userId)) {
            $queryText .= " AND t.user_id={$userId}";
        }

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $amount = empty($results[0]['program_amount']) ? 0.0 : $results[0]['program_amount'];
        return $amount;
    }

    /**
     * @param Connection $connection
     * @param $partnerId
     * @param $userId
     * @param $start
     * @param $end
     * @return float|string
     * @throws DBALException
     */
    private function getSumProgramAmount(Connection $connection, $partnerId, $userId, $start, $end) {
        $queryText = "SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $amount = empty($results[0]['program_amount']) ? 0.0 : $results[0]['program_amount'];
        return $amount;
    }

    private function calculateZScore($x, $avg, $std) {
        if (empty($avg) or empty($std)) {
            return "-11.00";
        }

        if (empty($x)) {
            return "-10.00";
        }

        $zScore = ($x - $avg) / ($std);
        $zScore = number_format($zScore, 2, '.', '');

        return $zScore;
    }

    private function zScoreTransactionCount(Connection $connection, $partnerIds, $currentUserId, $start, $end) {
        $whereTerms = array();

        if (is_array($partnerIds)) {
            if (1 < count($partnerIds)) {
                $partnerIdsList = implode(',', $partnerIds);
                $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
            } elseif (1 === count($partnerIds)) {
                $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
            }
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.t_count) AS `avg`, STD(x.t_count) AS `std`
            FROM (
                SELECT u.id u_id, COUNT(DISTINCT t.id) AS t_count
                FROM account_user u
                LEFT JOIN cashback_transaction t ON u.id=t.user_id
                WHERE {$where}
                GROUP BY u.id
                HAVING 0<t_count
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT u.id u_id, COUNT(DISTINCT t.id) t_count
            FROM account_user u
            LEFT JOIN cashback_transaction t ON u.id=t.user_id
            WHERE ( u.id = {$currentUserId} ) AND {$where}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['t_count']) ? 0.0 : floatval($results[0]['t_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreNumberOfPartnersVisitedForMainCategory(Connection $connection, $mainCategoryId, $currentUserId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.partner_count) AS `avg`, STD(x.partner_count) AS `std`
            FROM (
                SELECT tmp_t.user_id AS user_id, COUNT(DISTINCT pv.partner_id) AS partner_count
                FROM (
                    SELECT t.user_id AS user_id, COUNT(DISTINCT t.id) AS t_count
                    FROM cashback_transaction t
                    WHERE {$where}
                    GROUP BY t.user_id
                    HAVING 0<t_count
                ) tmp_t
                LEFT JOIN PartnerVisit pv ON tmp_t.user_id=pv.user_id
                LEFT JOIN Partner p ON pv.partner_id=p.id
                LEFT JOIN Category c ON p.main_category_id=c.id
                LEFT JOIN Category cp ON c.parent_id=cp.id
                WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))
                GROUP BY tmp_t.user_id
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT pv.user_id AS u_id, COUNT(DISTINCT pv.partner_id) AS partner_count
            FROM PartnerVisit pv
            LEFT JOIN Partner p ON pv.partner_id=p.id
            LEFT JOIN Category c ON p.main_category_id=c.id
            LEFT JOIN Category cp ON c.parent_id=cp.id
            WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId})) AND (pv.user_id = {$currentUserId}) AND (pv.user_id IS NOT NULL)";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['partner_count']) ? 0.0 : floatval($results[0]['partner_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreNumberOfPartnersVisited(Connection $connection, $partnerIds, $currentUserId, $start, $end) {
        $whereTerms = array();

        if (is_array($partnerIds)) {
            if (1 < count($partnerIds)) {
                $partnerIdsList = implode(',', $partnerIds);
                $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
            } elseif (1 === count($partnerIds)) {
                $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
            }
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.partner_count) AS `avg`, STD(x.partner_count) AS `std` 
            FROM (
                SELECT pv.user_id AS user_id, COUNT(DISTINCT pv.partner_id) AS partner_count
                FROM PartnerVisit pv
                WHERE {$where} AND (pv.user_id IS NOT NULL)
                GROUP BY pv.user_id
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT pv.user_id AS user_id, COUNT(DISTINCT pv.partner_id) AS partner_count
            FROM PartnerVisit pv
            WHERE {$where} AND (pv.user_id = {$currentUserId}) AND (pv.user_id IS NOT NULL)";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['partner_count']) ? 0.0 : floatval($results[0]['partner_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreVisitCount(Connection $connection, $partnerIds, $currentUserId, $start, $end) {
        $whereTerms = array();

        if (is_array($partnerIds)) {
            if (1 < count($partnerIds)) {
                $partnerIdsList = implode(',', $partnerIds);
                $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
            } elseif (1 === count($partnerIds)) {
                $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
            }
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.visit_count) AS `avg`, STD(x.visit_count) AS `std`
            FROM (
                SELECT tmp_t.user_id AS user_id, tmp_t.t_count AS t_count, COUNT(DISTINCT pv.id) AS visit_count
                FROM (
                    SELECT t.user_id AS user_id, COUNT(DISTINCT t.id) AS t_count
                    FROM cashback_transaction t
                    WHERE {$where}
                    GROUP BY t.user_id
                ) tmp_t
                LEFT JOIN PartnerVisit pv ON tmp_t.user_id=pv.user_id
                WHERE {$where}
                GROUP BY tmp_t.user_id
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT pv.user_id AS user_id, COUNT(DISTINCT pv.id) AS visit_count
            FROM PartnerVisit pv
            WHERE {$where} AND (pv.user_id = {$currentUserId}) AND (pv.user_id IS NOT NULL)";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['visit_count']) ? 0.0 : floatval($results[0]['visit_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreNumberOfPartnersSpentAt(Connection $connection, $partnerIds, $currentUserId, $start, $end) {
        $whereTerms = array();

        if (is_array($partnerIds)) {
            if (1 < count($partnerIds)) {
                $partnerIdsList = implode(',', $partnerIds);
                $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
            } elseif (1 === count($partnerIds)) {
                $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
            }
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.p_count) AS `avg`, STD(x.p_count) AS `std`
            FROM (
                SELECT u.id u_id, COUNT(DISTINCT t.partner_id) AS p_count
                FROM account_user u
                LEFT JOIN cashback_transaction t ON u.id=t.user_id
                WHERE {$where}
                GROUP BY u.id
                HAVING 0<p_count
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT u.id u_id, COUNT(DISTINCT t.partner_id) AS p_count
            FROM account_user u
            LEFT JOIN cashback_transaction t ON u.id=t.user_id
            WHERE ( u.id = {$currentUserId} ) AND {$where}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['p_count']) ? 0.0 : floatval($results[0]['p_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreProgramAmount(Connection $connection, $partnerIds, $currentUserId, $start, $end) {
        $whereTerms = array();

        if (is_array($partnerIds)) {
            if (1 < count($partnerIds)) {
                $partnerIdsList = implode(',', $partnerIds);
                $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
            } elseif (1 === count($partnerIds)) {
                $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
            }
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.t_sum_amount) AS `avg`, STD(x.t_sum_amount) AS `std`
            FROM (
                SELECT u.id u_id, COUNT(DISTINCT t.id) AS t_count, SUM(t.programAmount) t_sum_amount
                FROM account_user u
                LEFT JOIN cashback_transaction t ON u.id=t.user_id
                WHERE {$where}
                GROUP BY u.id
                HAVING 0<t_count
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT u.id u_id, SUM(t.programAmount) t_sum_amount
            FROM account_user u
            LEFT JOIN cashback_transaction t ON u.id=t.user_id
            WHERE ( u.id = {$currentUserId} ) AND {$where}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['t_sum_amount']) ? 0.0 : floatval($results[0]['t_sum_amount']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreVisitCountForMainCategory(Connection $connection, $mainCategoryId, $currentUserId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.visit_count) AS `avg`, STD(x.visit_count) AS `std`
            FROM (
                SELECT tmp_t.user_id AS user_id, tmp_t.t_count AS t_count, COUNT(DISTINCT pv.id) AS visit_count
                FROM (
                    SELECT t.user_id AS user_id, COUNT(DISTINCT t.id) AS t_count
                    FROM cashback_transaction t
                    GROUP BY t.user_id
                ) tmp_t
                LEFT JOIN PartnerVisit pv ON tmp_t.user_id=pv.user_id
                LEFT JOIN Partner p ON pv.partner_id=p.id
                LEFT JOIN Category c ON p.main_category_id=c.id
                LEFT JOIN Category cp ON c.parent_id=cp.id
                WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))
                GROUP BY tmp_t.user_id
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT pv.user_id AS u_id, COUNT(DISTINCT pv.id) AS visit_count
            FROM PartnerVisit pv
            LEFT JOIN Partner p ON pv.partner_id=p.id
            LEFT JOIN Category c ON p.main_category_id=c.id
            LEFT JOIN Category cp ON c.parent_id=cp.id
            WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId})) AND (pv.user_id = {$currentUserId}) AND (pv.user_id IS NOT NULL)";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['visit_count']) ? 0.0 : floatval($results[0]['visit_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function zScoreProgramAmountForMainCategory(Connection $connection, $mainCategoryId, $currentUserId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= t.`time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( t.`time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.t_sum_amount) AS `avg`, STD(x.t_sum_amount) AS `std`
            FROM (
                SELECT u.id u_id, COUNT(DISTINCT t.id) AS t_count, SUM(t.programAmount) t_sum_amount
                FROM cashback_transaction t
                LEFT JOIN account_user u ON t.user_id=u.id
                LEFT JOIN Partner p ON t.partner_id=p.id
                LEFT JOIN Category c ON p.main_category_id=c.id
                LEFT JOIN Category cp ON c.parent_id=cp.id
                WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))
                GROUP BY u.id
                HAVING 0<t_count
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT u.id u_id, SUM(t.programAmount) t_sum_amount
            FROM cashback_transaction t
            LEFT JOIN account_user u ON t.user_id=u.id
            LEFT JOIN Partner p ON t.partner_id=p.id
            LEFT JOIN Category c ON p.main_category_id=c.id
            LEFT JOIN Category cp ON c.parent_id=cp.id
            WHERE ( u.id = {$currentUserId} ) AND {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['t_sum_amount']) ? 0.0 : floatval($results[0]['t_sum_amount']);

        return $this->calculateZScore($x, $avg, $std);
    }

    public function zScoreTransactionCountForMainCategory(Connection $connection, $mainCategoryId, $currentUserId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= t.`time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( t.`time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText =
            "SELECT AVG(x.t_count) AS `avg`, STD(x.t_count) AS `std`
            FROM (
                SELECT u.id u_id, COUNT(DISTINCT t.id) AS t_count, SUM(t.programAmount) t_sum_amount
                FROM cashback_transaction t
                LEFT JOIN account_user u ON t.user_id=u.id
                LEFT JOIN Partner p ON t.partner_id=p.id
                LEFT JOIN Category c ON p.main_category_id=c.id
                LEFT JOIN Category cp ON c.parent_id=cp.id
                WHERE {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))
                GROUP BY u.id
            ) x";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $avg = empty($results[0]['avg']) ? 0.0 : floatval($results[0]['avg']);
        $std = empty($results[0]['std']) ? 0.0 : floatval($results[0]['std']);

        $queryText =
            "SELECT u.id u_id, COUNT(DISTINCT t.id) t_count
            FROM cashback_transaction t
            LEFT JOIN account_user u ON t.user_id=u.id
            LEFT JOIN Partner p ON t.partner_id=p.id
            LEFT JOIN Category c ON p.main_category_id=c.id
            LEFT JOIN Category cp ON c.parent_id=cp.id
            WHERE ( u.id = {$currentUserId} ) AND {$where} AND p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();

        $x = empty($results[0]['t_count']) ? 0.0 : floatval($results[0]['t_count']);

        return $this->calculateZScore($x, $avg, $std);
    }

    private function generalGetSumProgramAmount(Connection $connection, $partnerIds, $userId, $start, $end) {
        $whereTerms = array();

        if (1 < count($partnerIds)) {
            $partnerIdsList = implode(',', $partnerIds);
            $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
        } elseif (1 === count($partnerIds)) {
            $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
        }

        if (!empty($userId)) {
            $whereTerms[] = "( user_id = {$userId} )";
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE {$where}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $amount = empty($results[0]['program_amount']) ? 0.0 : $results[0]['program_amount'];
        return $amount;
    }

    /*
    private function getVisitCountForSubCategories(Connection $connection, $partnerId, $userId, $start, $end) {
        $subCategoryIds = $this->getSubCategoryIds($connection, $partnerId);
        $subCategoryIdsList = implode(',', $subCategoryIds);

        if (empty($subCategoryIdsList)) {
            return 0;
        }

        $queryText = "SELECT COUNT(DISTINCT PartnerVisit.id) AS visit_count FROM PartnerVisit LEFT JOIN Partner ON PartnerVisit.partner_id=Partner.id LEFT JOIN partner_category ON Partner.id=partner_category.partner_id WHERE partner_category.category_id IN ({$subCategoryIdsList}) AND user_id={$userId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $visitCount = empty($results[0]['visit_count']) ? 0 : intval($results[0]['visit_count']);

        return $visitCount;
    }
    */

    /**
     * @param Connection $connection
     * @param $mainCategoryId
     * @param $userId
     * @param $start
     * @param $end
     * @return int
     * @throws DBALException
     */
    private function getVisitCountForMainCategory(Connection $connection, $mainCategoryId, $userId, $start, $end) {
        $queryText = "SELECT COUNT(DISTINCT pv.id) AS visit_count FROM PartnerVisit pv LEFT JOIN Partner p ON pv.partner_id=p.id LEFT JOIN Category c ON p.main_category_id=c.id LEFT JOIN Category cp ON c.parent_id=cp.id WHERE p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId}))";

        if (!empty($userId)) {
           $queryText .= " AND pv.user_id={$userId}";
        }

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $visitCount = empty($results[0]['visit_count']) ? 0 : intval($results[0]['visit_count']);

        return $visitCount;
    }

    /**
     * @param Connection $connection
     * @param $partnerId
     * @param $userId
     * @param $start
     * @param $end
     * @return int
     * @throws DBALException
     */
    private function getVisitCount(Connection $connection, $partnerId, $userId, $start, $end) {
        $queryText = "SELECT COUNT(DISTINCT id) AS visit_count FROM PartnerVisit WHERE user_id={$userId} AND partner_id={$partnerId}";

        if ($start instanceof \DateTime) {
            $queryText .= " AND '{$start->format('Y-m-d')}'<=time";
        }

        if ($end instanceof \DateTime) {
            $queryText .= " AND time<'{$end->format('Y-m-d')}'";
        }

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $visitCount = empty($results[0]['visit_count']) ? 0 : intval($results[0]['visit_count']);

        return $visitCount;
    }

    private function generalGetVisitCount(Connection $connection, $partnerIds, $userId, $start, $end) {
        $whereTerms = array();

        if (1 < count($partnerIds)) {
            $partnerIdsList = implode(',', $partnerIds);
            $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
        } elseif (1 === count($partnerIds)) {
            $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
        }

        if (!empty($userId)) {
            $whereTerms[] = "( user_id = {$userId} )";
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT COUNT(DISTINCT id) AS visit_count FROM PartnerVisit WHERE {$where}";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $visitCount = empty($results[0]['visit_count']) ? 0 : intval($results[0]['visit_count']);
        return $visitCount;
    }

    private function statisticsGetVisitCount(Connection $connection, $partnerIds, $start, $end) {
        $whereTerms = array();

        if (1 < count($partnerIds)) {
            $partnerIdsList = implode(',', $partnerIds);
            $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
        } elseif (1 === count($partnerIds)) {
            $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT MAX(`count`) AS `max`, MIN(`count`) AS `min`, AVG(`count`) AS `avg`, SUM(`count`) AS `sum`, STD(`count`) AS `std` FROM ( SELECT COUNT(DISTINCT id) AS `count` FROM PartnerVisit WHERE {$where} GROUP BY user_id ) v";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $results = $results[0];
        return $results;
    }

    private function statisticsSumProgramAmount(Connection $connection, $partnerIds, $start, $end) {
        $whereTerms = array();

        if (1 < count($partnerIds)) {
            $partnerIdsList = implode(',', $partnerIds);
            $whereTerms[] = "( partner_id IN ({$partnerIdsList}) )";
        } elseif (1 === count($partnerIds)) {
            $whereTerms[] = "( partner_id = {$partnerIds[0]} )";
        }

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT MAX(`program_amount`) AS `max`, MIN(`program_amount`) AS `min`, AVG(`program_amount`) AS `avg`, SUM(`program_amount`) AS `sum`, STD(`program_amount`) AS `std` FROM ( SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE {$where} GROUP BY user_id ) s";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $results = $results[0];
        return $results;
    }

    private function statisticsVisitCountForMainCategory(Connection $connection, $mainCategoryId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT MAX(`visit_count`) AS `max`, MIN(`visit_count`) AS `min`, AVG(`visit_count`) AS `avg`, SUM(`visit_count`) AS `sum`, STD(`visit_count`) AS `std` FROM  (SELECT COUNT(DISTINCT pv.id) AS visit_count FROM PartnerVisit pv LEFT JOIN Partner p ON pv.partner_id=p.id LEFT JOIN Category c ON p.main_category_id=c.id LEFT JOIN Category cp ON c.parent_id=cp.id WHERE p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId})) AND {$where} GROUP BY pv.user_id ) c";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $results = $results[0];
        return $results;
    }

    private function statisticsSumProgramAmountForMainCategory(Connection $connection, $mainCategoryId, $start, $end) {
        $whereTerms = array();

        if ($start instanceof \DateTime) {
            $whereTerms[] = "( '{$start->format('Y-m-d')}' <= `time` )";
        }

        if ($end instanceof \DateTime) {
            $whereTerms[] = "( `time` < '{$end->format('Y-m-d')}' )";
        }

        $where = implode(' AND ', $whereTerms);

        $queryText = "SELECT MAX(`program_amount`) AS `max`, MIN(`program_amount`) AS `min`, AVG(`program_amount`) AS `avg`, SUM(`program_amount`) AS `sum`, STD(`program_amount`) AS `std` FROM (SELECT SUM(t.programAmount) AS program_amount FROM cashback_transaction t LEFT JOIN Partner p ON t.partner_id=p.id LEFT JOIN Category c ON p.main_category_id=c.id LEFT JOIN Category cp ON c.parent_id=cp.id WHERE p.status='active' AND ((c.id = {$mainCategoryId}) OR (cp.id = {$mainCategoryId})) AND {$where} GROUP BY t.user_id ) s";

        $statement = $connection->prepare($queryText);
        $statement->execute();
        $results = $statement->fetchAll();
        $results = $results[0];
        return $results;
    }

    private function calculateTarget($amount) {
        $amount = floatval($amount);
        $amount += 1;
        $target = log($amount);
        return $target;
    }
}