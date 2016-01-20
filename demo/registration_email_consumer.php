<?php

require_once 'EventQueueHelper.php';
$callback = function($msg){
    echo 'send email to ' . $msg['email'] . ',' . $msg['cnt'] . "\n";
    $ret = 1;
    if($ret == 0){
        //若返回0，表示消息处理完成
        return 0;
    }elseif($ret == 1){
        //若返回1，表示希望等待一段时间后再处理，所以建议休眠一段时间后再尝试获取，否则容易造成"死循环"
        sleep(1);
        return 1;
    }
};

$queue = new EventQueueHelper();
while(true){
    $ret = $queue->pop('registration', 'email', $callback);
}
