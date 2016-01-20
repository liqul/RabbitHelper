<?php

namespace RabbitHelper;
/**
 * author: liqul@outlook.com
 * date: 2015-06-02
 */
use RabbitHelper\RabbitMQHelper;
use RabbitHelper\EventQueue;
use RabbitHelper\RabbitMQQueueBase;

/**
 * RabbitMQNormalFanoutEventQueue支持多队列，每个队列一到多个消费者
 */
class RabbitMQFanoutEventQueue extends RabbitMQQueueBase implements EventQueue
{
    function __construct($hosts, $enable_confirm = true)
    {
        $this->_init($hosts, $enable_confirm);
    }
    
    /**
     * 插入一条消息
     * @param  string $str_event    消息内容
     * @param  string $str_push_key exchange名称
     * @return bool                 true表示成功
     */
    public function push($str_push_key, $str_event)
    {

        return $this->rabbitmq_helper->post(
            $str_push_key, 
            $str_event
        );
    }

    /**
     * 读出一条消息
     * @param  callable $callable_eventhandler  可调用回调函数
     * @param  bool $is_block                   是否采用阻塞模式
     * @param  string $str_pop_key              队列名称
     * @return bool                             true表示成功
     */
    public function pop($str_pop_key, $is_block=false, $callable_eventhandler=null)
    {
        if($is_block){
            return $this->rabbitmq_helper->listen(
                $callable_eventhandler, 
                $str_pop_key
            );
        }else{
            return $this->rabbitmq_helper->get(
                $str_pop_key
            );
        }
    }

    /**
     * 批量插入多条消息
     * @param  array  $arr_str_event    消息内容数组
     * @param  string $str_push_key     exchange名称
     * @return bool                     true表示成功
     */
    public function pushBatch($str_push_key, Array $arr_str_event)
    {
        if(empty($arr_str_event)){
            return true;
        }
        $arr_messages = array();
        foreach($arr_str_event as $str_event){
            $arr_messages[] = array(
                'body' => $str_event,
            );
        }
        return $this->rabbitmq_helper->postBatch($str_push_key, $arr_messages);
    }

    //not implemented
    public function popBatch($str_pop_key, $callable_eventhandler){}
}