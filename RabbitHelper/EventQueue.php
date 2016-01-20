<?php

namespace RabbitHelper;
/**
 * author: liqul@outlook.com
 * date: 2015-06-02
 * 接口：事件队列
 */
interface EventQueue
{
    /**
     * 往消息队列插入一条记录
     * @param  string $str_push_key 发布键
     * @param  string $str_event 消息描述字符串
     */
    public function push($str_push_key, $str_event);
    /**
     * 从消息队列读取一条记录
     * @param  array $str_pop_key  监听键
     * @param  bool $is_block 是否阻塞模式
     * @param  callable $callable_eventhandler 回调函数
     */
    public function pop($str_pop_key, $is_block=false, $callable_eventhandler=null);
    /**
     * 往消息队列插入多条记录
     * @param  string $str_push_key 路由键
     * @param  array $arr_str_events 数组，每一条记录是消息描述字符串
     */
    public function pushBatch($str_push_key, Array $arr_str_events);
    /**
     * 从消息队列读取多条记录
     * @param  array $str_pop_key  监听键
     * @param  callable $callable_eventhandler 回调函数
     */
    public function popBatch($str_pop_key, $callable_eventhandler);
}