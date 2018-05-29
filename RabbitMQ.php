<?php

/**
 * PHP RabbitMQ消息操作类
 * 
 * Amqp消息队列系统
 * http://php.net/manual/pl/book.amqp.php
 * @author    huxg(905988767@qq.com)
 * @version   v1.0.1
 */
class RabbitMQ
{
	private $config  = array();
	
	//交换机名称
    private $change_name = '';
	
	//队列名称
    private $queue_name = '';
	
	//路由名称
    private $route_name = '';
	
	private $_connect  = null;
	private $_channel  = null;
	private $_queue    = null;
	private $_exchange = null;
	
	private $autoack   = false;
	
	/**
	 * 初始化配置
	 */
	public function __construct($config = array(), $change = '', $queue = '', $route = '') 
	{
		$this->init($config,$change,$queue,$route);
	}
	
	public function init($config,$change,$queue,$route)
	{
		if(is_array($config)){
            $this->config = $config;
			$this->connect();
        }else{
			throw new Exception('配置参数错误！',500);
			exit();
		}
		
		if($change){
			$this->change_name = $change;
			$this->channel();
		}else{
			throw new Exception('交换机名称不能为空！',500);
			exit();
		}
		
		if($queue){
			$this->queue_name  = $queue;
			$this->queue();
		}else{
			throw new Exception('队列名称不能为空！',500);
			exit();
		}
		
		if($route){
			$this->route_name  = $route;
			$this->route();
		}else{
			throw new Exception('路由名称不能为空！',500);
			exit();
		}
	}
	
	/**
	 * 创建连接
	 */
	private function connect() 
	{
		if (!$this->_connect) 
		{    
			try {
                $this->_connect = new \AMQPConnection($this->config);
                $this->_connect->connect();
            } catch (AMQPConnectionException $e) 
			{
                throw new Exception('Rabbitmq 链接失败',500);
            }
		} 
	}
	
	/**
	 * 创建交换机
	 */
	private function channel() 
	{	
		if($this->_connect && $this->change_name)
		{
			$this->_channel  = new \AMQPChannel($this->_connect);
			
			$this->_exchange =  new \AMQPExchange($this->_channel);    
			$this->_exchange->setName($this->change_name);
			
			$this->_exchange->setType(AMQP_EX_TYPE_DIRECT);
			$this->_exchange->setFlags(AMQP_DURABLE);
			@$this->_exchange->declare();
		}
	}
	
	/**
	 * 创建队列
	 */
	private function queue() 
	{
		if($this->_channel && $this->queue_name) 
		{
			$this->_queue = new \AMQPQueue($this->_channel);  
			$this->_queue->setName($this->queue_name);    
			$this->_queue->setFlags(AMQP_DURABLE); //持久化   
			@$this->_queue->declare();
		}
	}
	
	/**
	 * 创建路由
	 */
	private function route() 
	{
		if($this->_queue)
		{
			$this->_queue->bind($this->change_name, $this->route_name);  
		}
	}
	
	/**
	 * 接受消息
	 * @$callback 回调方法
	 * @param $autoack 自动应答
	 */
	public function receive($callback, $autoack=false) 
	{
		//阻塞模式接收消息
		
		$this->autoack = $autoack;
		
		while(True)
		{  
			if($autoack)
			{
				$this->_queue->consume($callback, AMQP_AUTOACK); //自动ACK应答  
			}else{
				$this->_queue->consume($callback);  
			}
		}
		$this->close();
	}
	
	public function processMessage($envelope, $queue) 
	{  
		$msg = $envelope->getBody();  
		echo $msg."\n"; //处理消息
		
		if($this->autoack==false)
		{
			$queue->ack($envelope->getDeliveryTag()); //手动发送ACK应答
		} 
	}

	/**
	 * 发送消息
	 * @param $autoack 自动应答
	 */
	public function send($message) 
	{
		//发送消息
		if($message)
		{
			if(is_array($message))
			{
				$message = json_encode($message);
			}else{
				$message = trim(strval($message));
			}
			//开始事务
			$this->_channel->startTransaction(); 
			$this->_exchange->publish($message, $this->route_name); 
			//提交事务 			
			return $this->_channel->commitTransaction();
		}
	}
	
	/** 
	 * 关闭连接
	 */ 
	public function close() 
	{
        if ($this->_connect) 
		{
            $this->_connect->disconnect();
        }
    }
	
	/** 
	 * 释放链接资源
	 */ 
	public function __destruct() 
	{
        $this->close();
    }
}
