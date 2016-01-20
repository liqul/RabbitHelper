<?php

namespace RabbitHelper;
/**
 * author: liqul@outlook.com
 * date: 2015-06-02
 */
use RabbitHelper\RabbitMQHelper;

class RabbitMQQueueBase
{
    protected $rabbitmq_helper = null;
    protected function _init($hosts, $enable_confirm = true)
    {
        $arr_hosts = explode(',', $hosts);
        $host_list = array();
        foreach($arr_hosts as $str_host){
            $fields = explode(':', trim($str_host));
            $host_list[] = array(
                'host' => $fields[0], 
                'port' => $fields[1],
                'user' => $fields[2],
                'passwd' => $fields[3],
                'vhost' => $fields[4]
            );
        }
        $this->rabbitmq_helper = new RabbitMQHelper(
            array(
                'hosts'=>$host_list, 
                'enable_confirm'=>$enable_confirm
                )
        );
    }
}