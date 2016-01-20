<?php

namespace RabbitHelper;
/**
 * author: liqul@outlook.com
 * date: 2015-06-02
 */
use RabbitHelper\RabbitMQHelper;
use RabbitHelper\EventQueue;

/**
 * SimpleEventQueue支持最基本的单队列多消费者竞争消费场景
 */
class RabbitMQAnonymousEventQueue extends RabbitMQQueueBase implements EventQueue
{
    function __construct($hosts, $enable_confirm = true)
    {
        $this->_init($hosts, $enable_confirm);
    }
    /**
     * 插入一条消息
     * @param  string $str_event    消息内容
     * @param  string $str_push_key 队列名称
     * @return bool               true表示成功
     */
    public function push($str_push_key, $str_event)
    {
        //这里exchange名称为''，RabbitMQ会直接根据队列名称路由
        return $this->rabbitmq_helper->post('', $str_event, $str_push_key);
    }
    /**
     * 读取一条消息
     * @param  callable $callable_taskhandler 可执行回调函数
     * @param bool $is_block 是否采用阻塞模式
     * @param  string $str_pop_key       队列名称
     * @return bool                       true表示成功
     */
    public function pop($str_pop_key, $is_block=false, $callable_eventhandler=null)
    {
        if($is_block){
            return $this->rabbitmq_helper->listen(
                $callable_taskhandler, $str_pop_key
            );
        }else{
            return $this->rabbitmq_helper->get(
                $str_pop_key
            );
        }
    }
    /**
     * 批量插入多条消息
     * @param  Array  $arr_str_task 数组，每条是一条消息
     * @param  string $str_push_key 队列名称
     * @return bool               true表示成功
     */
    public function pushBatch($str_push_key, Array $arr_str_task)
    {
        if(empty($arr_str_task)){
            return true;
        }
        $arr_message = array();
        foreach($arr_str_task as $str_task){
            $arr_message[] = array('body' => $str_task, 'routing_key' => $str_push_key);
        }
        return $this->rabbitmq_helper->postBatch('', $arr_message);
    }

    // not implemented
    public function popBatch($str_pop_key, $callable_eventhandler){}
}