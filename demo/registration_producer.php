<?php

require_once 'EventQueueHelper.php';

$queue = new EventQueueHelper();

for($i = 0; $i < 100; $i++){
    $queue->push('registration', 
        array('name' => 'jay', 'phone' => '68945673', 'email' => 'abc@outlook.com', 'cnt' => $i)
    );
    echo $i . "\n";
    sleep(1);
}
