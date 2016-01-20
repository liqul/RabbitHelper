<?php

require_once 'EventQueueHelper.php';

$callback = function($msg){
    echo 'send sms to ' . $msg['phone'] . "\n";
};

$queue = new EventQueueHelper();
while(true){
    $msg = $queue->pop('registration', 'sms');
    if($msg !== null){
        echo 'send sms to ' . $msg['phone'] . ',' . $msg['cnt'] . "\n";
    }else{
        echo "empty queue\n";
    }
    sleep(1);
}
