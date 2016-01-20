<?php

namespace RabbitHelper\Exception;

/**
 * 用于表示自定义callback内部发生的异常
 */
class RabbitMQExceptionBase extends \Exception { 
    // custom string representation of object
    public function __toString() {
        return get_called_class() . ": [{$this->code}]: {$this->message}\n";
    }
}