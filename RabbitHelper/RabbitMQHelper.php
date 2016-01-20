<?php

namespace RabbitHelper;
/**
 * author: liqul@outlook.com
 * date: 2015-06-01
 */
use PhpAmqpLib\Connection\AMQPConnection;
use PhpAmqpLib\Message\AMQPMessage;
use PhpAmqpLib\Exception\AMQPProtocolChannelException;
use RabbitMQHelper\Exception\RabbitMQParameterException;
use RabbitMQHelper\Exception\RabbitMQCallbackException;
use RabbitMQHelper\Exception\RabbitMQConnectionFailException;
use \Exception;

/**
 * true打开内部调试信息
 */
define('AMQP_DEBUG', false);

class RabbitMQHelper
{
    /**
     * exchange类型
     * 1. fanout广播
     * 2. direct根据队列名单播
     * 3. topic根据规则多播
     */
    const EXCHANGE_FANOUT = 'fanout';
    const EXCHANGE_DIRECT = 'direct';
    const EXCHANGE_TOPIC = 'topic';

    /**
     * 消息处理符号
     */
    const FLAG_OK = 0;
    const FLAG_REQUEUE = 1;

    /**
     * 重试次数
     */
    const MAX_RETRY = 1;

    //connection
    private $conn = null;
    //channel
    private $channel = null;
    //rabbitmq cluster的host列表
    private $host_list = array();
    //是否定义consumer
    private $is_consumer_defined = false;
    //接收到事件的callback
    private $callback = null;
    //设置message参数
    private $message_parameters = array(
        'content_type' => 'application/json', //Better if the content is json
        'delivery_mode' => 2,   //Persistent that can survive 
        'delivery_tag' => ''    //自动生成
    );
    //是否开启confirm模式
    private $enable_confirm = false;
    //记录重试次数
    private $retry_count = 0;
    //记录当前已尝试失败的配置
    private $tried_hosts = array();

    function __construct($params = null)
    {
        $this->_initParams($params);//初始化参数
        $this->_initConn();//初始化连接
    }

    private function _initParams($params)
    {
        if(isset($params['hosts'])){
            $this->host_list = $params['hosts'];
            foreach($this->host_list as $conf){
                $this->_checkConf($conf);
            }
            shuffle($this->host_list);
        }else{
            throw new RabbitMQParameterException('No RabbitMQ hosts provided');
        }
        if(isset($params['enable_confirm'])){
            $this->enable_confirm = $params['enable_confirm'];
        }
        if(isset($params['message_parameters'])){
            $this->setMessageParameters($params['message_parameters']);
        }
    }

    /**
     * 设置队列message->body内容类型，常用的包括：
     * @param array(
     *  content_type => text/plain | application/json,
     *  delivery_mode => 2 (persistent) | 1 (transient),
     *  reply_to => string, usually used to convey a queue name
     *  correlation_id => a id usually used for RPC
     *  )
     */
    public function setMessageParameters($parameters)
    {
        $this->message_parameters = $parameters;
    }

    /**
     * 初始化连接，如果无法连接则抛出异常
     */
    private function _initConn($ex = null)
    {
        if(!empty($ex)){
            echo "Re-initialize connection due to " . $ex->getMessage() . "\n";
        }
        //先关闭可能已经存在的连接
        $this->_shutdown($this->channel, $this->conn);
        foreach($this->host_list as $conf){
            //如果所有配置都已经尝试过了
            if(count($this->host_list) == count($this->tried_hosts)){
                throw new RabbitMQConnectionFailException("No valid host among " 
                    . var_export($this->host_list, true));
            }
            if(in_array($conf, $this->tried_hosts)){
                continue;
            }
            try{
                echo "Connecting to {$conf['host']}:{$conf['port']}\n";
                $this->tried_hosts[] = $conf;
                $this->conn = new AMQPConnection(
                    $conf['host'], //ip地址
                    $conf['port'], //端口
                    $conf['user'], //用户名
                    $conf['passwd'], //密码
                    $conf['vhost']); //虚拟host
                $this->channel = $this->conn->channel();
                //启动发送确认
                if($this->enable_confirm){
                    $this->channel->confirm_select();
                }
                $this->is_consumer_defined = false;
                //Log "Connection established to {$conf['host']}:{$conf['port']}\n";
                //一旦成功连接则清空已尝试的host列表
                $this->tried_hosts = array();
                echo "Successfully connected to {$conf['host']}:{$conf['port']}\n";
                break;
            }catch(Exception $ex){
                //connection to conf failed
                echo "Fail to connect to {$conf['host']}:{$conf['port']} due to {$ex->getMessage()}\n";
            }
        }
    }

    /**
     * 检查配置输入字段
     * @param  array $conf array('host', 'port', 'user', 'passwd', 'vhost')
     * @return null       无返回
     */
    private function _checkConf($conf)
    {
        $conf_fields = array('host', 'port', 'user', 'passwd', 'vhost');
        foreach ($conf_fields as $field_name) {
            if(!isset($conf[$field_name])){
                throw new RabbitMQParameterException("$field_name not specified in hosts");
            }
        }
    }

    /**
     * 函数调用入口，实现异常处理
     * @param  string $method 函数名
     * @param  array $args    参数
     * @return mix            函数返回值
     */
    public function __call($method, $args)
    {
        while(true){
            try{
                return call_user_func_array(array($this, '_' . $method), $args); 
            }catch(RabbitMQParameterException $paramex){
                //如果参数异常直接向外抛出
                throw($paramex);
            }catch(RabbitMQCallbackException $callbackex){
                //如果callback抛出异常直接向外抛出
                throw($callbackex);
            }catch(Exception $ex){
                //尝试其它的连接配置，直到尝试完所有后抛出异常
                $this->_initConn($ex);
            }
        }
    }

    /**
     * 发出一个事件到事件exchange
     * @param  string $exchange_name exchange名称
     * @param  string $message_body  事件内容
     * @param  string $routing_key   路由关键词，若exchange_name=''，则表示direct路由模式，这里填queue_name
     * @return bool                true表示成功
     */
    private function _post($exchange_name, $message_body, $routing_key = null)
    {
        $msg = new AMQPMessage($message_body, $this->message_parameters);
        $this->channel->basic_publish($msg, $exchange_name, $routing_key);
        if($this->enable_confirm){
            $this->channel->wait_for_pending_acks();
        }
    }

    /**
     * 批量发出事件到事件exchange
     * @param  string $exchange_name exchange名称
     * @param  array  $arr_message   array(array('body', 'routing_key(direct/topic必须)'))
     * @param  string $exchange_type fanout | direct | topic
     * @return bool                true
     */
    private function _postBatch($exchange_name, Array $arr_message)
    {
        foreach($arr_message as $message){
            $msg = new AMQPMessage($message['body'], $this->message_parameters);
            if(isset($message['routing_key'])){
                $this->channel->batch_basic_publish($msg, $exchange_name, $message['routing_key']);
            }else{
                $this->channel->batch_basic_publish($msg, $exchange_name);
            }
        }
        $this->channel->publish_batch();
        if($this->enable_confirm){
            $this->channel->wait_for_pending_acks();
        }
    }

    /**
     * 注册接收到事件的callback
     * @param  callable  $callback 可执行的callback
     * @param  bool $no_ack   true表示不需要接收方回ack
     */
    private function registerCallback($callback, $no_ack = true)
    {
        $requeue_flag = self::FLAG_REQUEUE;
        $this->callback = function($msg) use ($callback, $no_ack, $requeue_flag) {
            $action_flag = null;
            try{
                //把消息内容解析出来
                $msg_arr = json_decode($msg->body, true);
                $action_flag = call_user_func($callback, $msg_arr);
            }catch(Exception $ex){
                //抛出异常，由外部处理
                throw new RabbitMQCallbackException($ex->getMessage());
            }
            if(!$no_ack){
                if($action_flag === $requeue_flag){
                    //支持暂时将消息放回队列，等待以后处理，慎用
                    $msg->delivery_info['channel']->basic_nack(
                        $msg->delivery_info['delivery_tag'],
                        false,  // is bulk reject
                        true    // is requeue
                    );
                }else{
                    $msg->delivery_info['channel']->basic_ack($msg->delivery_info['delivery_tag']);
                };
            }
        };
    }

    /**
     * 获取一个事件，阻塞调用，直到获取到一个事件为止
     * @param  callable     $callback      可执行的callback
     * @param string        $queue_name    定义queue的名称，如果不定义则系统随机产生一个名称
     */
    private function _listen($callback, $queue_name, $noack_consume=false, $prefetch_count=1)
    {
        //检查callback
        if(!$this->callback){
            if(!is_callable($callback)){
                throw new RabbitMQParameterException("callback is not callable");
            }
            $this->registerCallback($callback, $noack_consume);
        }
        
        if(!$this->is_consumer_defined){
            $this->channel->basic_consume(
                $queue_name,        //queue: Queue from where to get the messages
                '',                 //consumer_tag: Consumer identifier
                false,              //no_local: Don't receive messages published by this consumer.
                $noack_consume,     //no_ack: Tells the server if the consumer will acknowledge the messages.
                false,              //exclusive: Request exclusive consumer access, meaning only this consumer can access the queue
                false,              //nowait:
                $this->callback     //callback: A PHP Callback
            );
            //确保每次只从队列中取一个消息
            //注意: 只有noack_consume=false的时候才有效，因为qos限制的是"待确认"的消息数量
            //而noack_consume=true的时候是没有待确认的消息的
            $this->channel->basic_qos(null, $prefetch_count, null);
            $this->is_consumer_defined = true;
        }
        if($this->callback){
            $this->channel->wait();
        }
    }

    /**
     * 调用basic_get获取消息
     * @param  string  $queue_name 队列名称
     * @param  boolean $noack_get  true表示不需要返回ack
     * @return message | null              如果没有消息返回null
     */
    private function _get($queue_name, $noack_get = false)
    {
        $msg = $this->channel->basic_get($queue_name, $noack_get);
        if(is_object($msg) && !$noack_get){
           $this->channel->basic_ack($msg->delivery_info['delivery_tag']); 
        }
        return $msg;
    }

    function __deconstruct()
    {
        $this->_shutdown($this->channel, $this->conn);
    }

    private function _shutdown($channel, $connection){
        try{
            if($channel){
                $channel->close();
            }
        }catch(Exception $ex){}
        try{
            if($connection){
                $connection->close();
            }
        }catch(Exception $ex){}
    }

    /**
     * 新建队列
     * @param  string $name        queue名称
     * @param  bool $auto_delete 是否自动删除
     * @param  array $status     队列状态
     * @return bool              true表示成功
     */
    private function _declareQueue($name, $auto_delete, &$status = null, $delete = false)
    {
        if($delete){
            $this->_deleteQueue($name);
        }
        $status = $this->channel->queue_declare(
            $name, //queue name
            false, //passive 
            true, //durable, true means the queue will survive server restarts
            false, //exclusive, true means the queue can not be accessed in other channels
            $auto_delete //true means the queue will be deleted once channel is gone
        );
        $this->is_queue_defined = true;
    }

    /**
     * 删除队列
     * @param  string $name queue名称
     * @return bool       true表示成功
     */
    private function _deleteQueue($name)
    {
        $this->channel->queue_delete($name);
    }

    /**
     * 清空队列
     * @param  string $name queue名称
     * @return bool       true表示成功
     */
    private function _purgeQueue($name)
    {
        try{
            $this->channel->queue_purge($name);
            return true;
        }catch(AMQPProtocolChannelException $aex){
            return false;
        }
    }

    /**
     * 定义Exchange
     * @param  string $name exchange名称
     * @param  string $type exchange类型
     * @return bool       true表示成功
     */
    private function _declareExchange($name, $type, $delete = false)
    {
        if($delete){
            $this->_deleteExchange($name);
        }
        $this->channel->exchange_declare(
            $name, //exchange name
            $type, //type: fanout, direct, topic
            false, //passive false
            true, //durable, same with queue
            false //auto_delete, same with queue
        );
        $this->is_exchange_defined = true;
    }

    /**
     * 删除exchange
     * @param  string $name exchange名称
     * @return bool       true表示成功
     */
    private function _deleteExchange($name)
    {
        $this->channel->exchange_delete($name);
    }


    /**
     * 将exchange与queue绑定起来
     * @param  string     $exchange_name exchange名称
     * @param  string     $queue_name    queue名称
     * @param  Array|null $binding_keys  绑定key数组
     * @return bool                    true表示成功
     */
    private function _bindExchangeQueue($exchange_name, $queue_name, Array $binding_keys = null)
    {
        if($binding_keys){
            foreach($binding_keys as $binding_key){
                $this->channel->queue_bind($queue_name, $exchange_name, $binding_key);
            }
        }else{
            $this->channel->queue_bind($queue_name, $exchange_name);
        }
    }

    /**
     * 将exchange与queue解绑
     * @param  string     $exchange_name exchange名称
     * @param  string     $queue_name    queue名称
     * @param  Array|null $binding_keys  绑定key数组
     * @return bool                    true表示成功
     */
    private function _unbindExchangeQueue($exchange_name, $queue_name, Array $binding_keys = null)
    {
        if($binding_keys){
            foreach($binding_keys as $binding_key){
                $this->channel->queue_unbind($queue_name, $exchange_name, $binding_key);
            }
        }else{
            $this->channel->queue_unbind($queue_name, $exchange_name);
        }
    }
}

