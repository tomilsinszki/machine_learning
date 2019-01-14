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

class PendingTransactionsReportCommand extends ContainerAwareCommand
{
    protected function configure() {
        $this->setName('rabattcorner:pending:transactions');
        $this->setDescription('Pending Transaction List');
    }

    protected function execute(InputInterface $input, OutputInterface $output) {
        /** @var EntityManager $em */
        $em = $this->getContainer()->get('doctrine')->getManager();

        $periods = array();

        $now = new \DateTime();
        $currentDay = new \DateTime();
        $currentDay->setDate(2017, 1, 1);
        $currentDay->setTime(0, 0, 0, 0);

        $shouldRun = true;
        while ($shouldRun) {
            $year = $currentDay->format("Y");

            $weekNumber = ltrim($currentDay->format("W"), '0');
            $previousWeekNumber = ($weekNumber-1);

            $periodKey = "{$year}-{$weekNumber}";
            $previousPeriodKey = "{$year}-{$previousWeekNumber}";

            $isNewPeriod = !isset($periods[$periodKey]);
            $hasPreviousPeriod = isset($periods[$previousPeriodKey]['start']);

            // TODO: hack fix
            /*
            if ($periodKey === '2018-1') {
                $periods['2017-52'] = array(
                    'start' => new \DateTime('2017-12-25 00:00:00'),
                    'end' => new \DateTime('2018-01-01 00:00:00'),
                );
            }
            */

            if ($isNewPeriod) {
                $periods[$periodKey] = ['start' => clone $currentDay];

                if ($hasPreviousPeriod) {
                    $periods[$previousPeriodKey]['end'] = clone $currentDay;
                }
            }

            $currentDay->modify('+1 day');
            if ($now < $currentDay) {
                $shouldRun = false;
            }
        }

        array_shift($periods);
        array_pop($periods);

        // TODO: hack fix
        $periods['2017-51']['end'] = new \DateTime('2017-12-25 00:00:00');

        /*
        $periods['2017-52'] = array(
            'start' => new \DateTime('2017-12-25 00:00:00'),
            'end' => new \DateTime('2018-01-01 00:00:00'),
        );
        */

        array_splice($periods, 51, 0, array(array(
            'start' => new \DateTime('2017-12-25 00:00:00'),
            'end' => new \DateTime('2018-01-01 00:00:00'),
        )));

        /*
        var_dump($periods['2017-51']);
        var_dump($periods['2017-52']);
        exit();
        */

        $periods['2018-52']['end'] = new \DateTime('2018-12-31 00:00:00');

        //print_r($periods);exit();

        $statement = $em->getConnection()->prepare("SELECT id FROM account_user WHERE account_user.id AND account_user.locked = 0 ORDER BY id");
        $statement->execute();
        $userIdResults = $statement->fetchAll();

        foreach($userIdResults as $userIdResult) {
            $userId = intval($userIdResult['id']);

            $firstAndLastContactIdStatement = $em->getConnection()->prepare("SELECT MIN(contact_id) AS `first`, MAX(contact_id) AS `last` FROM user_contact WHERE user_id={$userId}");
            $firstAndLastContactIdStatement->execute();
            $firstAndLastContactResult = $firstAndLastContactIdStatement->fetchAll();

            $firstContactId = $firstAndLastContactResult[0]['first'];
            $lastContactId = $firstAndLastContactResult[0]['last'];

            $signupDateStatement = $em->getConnection()->prepare("SELECT `time` FROM Contact WHERE id={$firstContactId}");
            $signupDateStatement->execute();
            $signupDateString = $signupDateStatement->fetchColumn(0);
            $signupDateString = substr($signupDateString, 0, 10);

            $contactDetailsStatement = $em->getConnection()->prepare("SELECT `title`, `postalcode` FROM Contact WHERE id={$lastContactId}");
            $contactDetailsStatement->execute();
            $contactDetailsResult = $contactDetailsStatement->fetchAll();
            $postalCode = $contactDetailsResult[0]['postalcode'];
            $gender = $contactDetailsResult[0]['title'];

            if ($gender === 'male') {
                $gender = '1';
            } elseif ($gender === 'female') {
                $gender = '2';
            } else {
                $gender = '';
            }

            $partnersStatement = $em->getConnection()->prepare("SELECT id FROM Partner");
            $partnersStatement->execute();
            $partnerIdResults = $partnersStatement->fetchAll();

            foreach ($partnerIdResults as $partnerIdResult) {
                $partnerId = intval($partnerIdResult['id']);

                foreach ($periods as $periodName => $period) {
                    //var_dump($periodName);exit();

                    /** @var \DateTime $start */
                    $start = $period['start'];

                    /** @var \DateTime $end */
                    $end = $period['end'];

                    $startString = "{$start->format('Y-m-d H:i:s')}";
                    $endString = "{$end->format('Y-m-d H:i:s')}";

                    // TODO: continue if signupDate is later than start of period: signupDateString < startString
                    // $startString
                    // $signupDateString

                    /*
                    $signupDateTime = new \DateTime("{$signupDateString} 00:00:00");
                    var_dump($start);
                    var_dump($signupDateTime);
                    exit();
                    */

                    $signupDateTime = new \DateTime("{$signupDateString} 00:00:00");

                    if ($end->getTimestamp() <= $signupDateTime->getTimestamp()) {
                        continue;
                    }

                    $year = $start->format("Y");
                    $weekNumber = ltrim($start->format("W"), '0');
                    //echo("{$year}, {$weekNumber}\n");

                    $transactionStatement = $em->getConnection()->prepare("SELECT SUM(programAmount) AS program_amount FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId} AND '{$startString}'<=time AND time<='{$endString}'");
                    $transactionStatement->execute();
                    $transactionProgramAmountResults = $transactionStatement->fetchAll();
                    $transactionSumProgramAmount = empty($transactionProgramAmountResults[0]['program_amount']) ? 0.0 : $transactionProgramAmountResults[0]['program_amount'];

                    // TODO: add week number
                    echo("userId,{$userId},partnerId,{$partnerId},year,{$year},week,{$weekNumber},start,{$start->format('Y-m-d')},end,{$end->format('Y-m-d')},sum,{$transactionSumProgramAmount},signup,{$signupDateString},postalcode,{$postalCode},gender,{$gender}\n");
                }
            }
        }






        /*
        // TODO: maybe we should also include locked users?
        $statement = $em->getConnection()->prepare("SELECT id FROM account_user WHERE account_user.locked = 0 ORDER BY id");
        $statement->execute();
        $userIdResults = $statement->fetchAll();

        $now = new \DateTime();

        foreach($userIdResults as $userIdResult) {
            $userId = intval($userIdResult['id']);

            $currentDay = new \DateTime();
            $currentDay->setDate(2018, 1, 1);
            $currentDay->setTime(0, 0, 0, 0);

            $shouldRun = true;
            while ($shouldRun) {
                // TODO: implement
                echo("{$userId} {$currentDay->format('Y-m-d')} ({$currentDay->format("W")})\n");

                $partnersStatement = $em->getConnection()->prepare("SELECT id FROM Partner");
                $partnersStatement->execute();
                $partnerIdResults = $partnersStatement->fetchAll();

                foreach ($partnerIdResults as $partnerIdResult) {
                    $partnerId = intval($partnerIdResult['id']);

                    $transactionStatement = $em->getConnection()->prepare("SELECT * FROM cashback_transaction WHERE partner_id={$partnerId} AND user_id={$userId} AND '{$currentDay->format('Y-m-d')} 00:00:00'<=time AND time<='2018-09-16 23:59:59'");
                    $transactionStatement->execute();
                    $transactionIdResults = $transactionStatement->fetchAll();
                }

                $currentDay->modify('+1 day');
                if ($now < $currentDay) {
                    $shouldRun = false;
                }
            }
        }
        */
    }
}