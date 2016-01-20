<?php

require_once '../vendor/autoload.php';
use RabbitHelper\RabbitMQFanoutEventQueue;

final class EventQueueHelper
{
    private $rabbitmq_event_queue = null;
    private $exchange_name = '';
    private $queue_name = '';
    private $hosts = '';
    private $handler = null;

    function __construct(){
        $this->hosts = '127.0.0.1:5672:user:pass:vhost,127.0.0.1:5673:user:pass:vhost';
        $this->rabbitmq_event_queue = new RabbitMQFanoutEventQueue($this->hosts, true);
    }

    /**
     * 发送消息
     * @param  string       $destination 消息发送对象
     * @param  array        $message 消息内容是key:value形式的数组；若批量发送则为一个消息数组
     * @param  bool         $is_batch 是否批量发送
     * @return bool          true表示成功
     */
    public function push($destination, Array $message)
    {
        if(empty($destination)){
            throw new Exception(__CLASS__ . '.' . __FUNCTION__ . ": no destination specified");
        }
        if(empty($message)){
            return true;
        }
        $this->exchange_name = $destination . '-exchange';
        $message_json = json_encode($message);
        return $this->rabbitmq_event_queue->push($this->exchange_name, $message_json);
    }

    /**
     * 批量发送消息
     * @param  string       $destination 消息发送对象
     * @param  array        $messages 消息数组
     * @param  bool         $is_batch 是否批量发送
     * @return bool          true表示成功
     */
    public function pushBatch($destination, Array $messages)
    {
        if(empty($destination)){
            throw new Exception(__CLASS__ . '.' . __FUNCTION__ . ": no destination specified");
        }
        if(empty($messages)){
            return true;
        }
        $this->exchange_name = $destination . '-exchange';
        $messages_json = array();
        foreach($messages as $message){
            $message_json = json_encode($message);
            $messages_json[] = $message_json;
        }
        return $this->rabbitmq_event_queue->pushBatch($this->exchange_name, $messages_json);
    }

    /**
     * 处理消息
     * @param  string $source 来源对象名称
     * @param  string $action 消费方式名称，消费方式相同的消费者处于竞争关系
     * @param  callable $callback 回调函数
     * @return bool               true表示成功
     */
    public function pop($source, $action, $callback=null){
        if($callback !== null && !is_callable($callback)){
            throw new Exception(__CLASS__ . '.' . __FUNCTION__ . ": no callback");
        }
        if(empty($action)){
            throw new Exception(__CLASS__ . '.' . __FUNCTION__ . ": no action specified");
        }
        if(empty($source)){
            throw new Exception(__CLASS__ . '.' . __FUNCTION__ . ": no source specified");
        }
        $this->queue_name = $source . '-' . $action . '-queue';
        if($callback !== null){
            $this->handler = $callback;
            $this->rabbitmq_event_queue->pop($this->queue_name, true, $this->handler);
        }else{
            $msg = $this->rabbitmq_event_queue->pop($this->queue_name, false);
            return empty($msg)? null : json_decode($msg->body, true);
        }
    }
}