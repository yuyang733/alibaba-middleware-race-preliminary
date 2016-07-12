package com.alibaba.middleware.race.rocketmq;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;

/**
 * 
 * @author yuyang
 *
 *
 */

public class ConsumerFactory {
	private static final Logger LOG = Logger.getLogger(ConsumerFactory.class);
	
	private static Map<String, DefaultMQPushConsumer> consumers = new ConcurrentHashMap<String, DefaultMQPushConsumer>();				//缓冲池
	
	public static synchronized DefaultMQPushConsumer getConsumer(RocketMQConsumerConfig rocketmqConsumerConf, MessageListenerOrderly listener) throws MQClientException{
		Set<String> topicSet = rocketmqConsumerConf.getTopic();
		String tag 	= 	rocketmqConsumerConf.getTag();
		String consumerGroup = rocketmqConsumerConf.getGroup();
		String key = "";
		for(String topic : topicSet){
			key += topic;
		}
		key += "@" + consumerGroup;
		String namesrv = rocketmqConsumerConf.getNamesrv();
		
		DefaultMQPushConsumer consumer = consumers.get(key);
		
		if (null != consumer){
			if(consumer.getMessageListener() == listener){
				return consumer;
			}
			else{
				LOG.info("Consumer of " + key + "has been created, don't recreate it");
				return null;
			}
		}
		
		consumer = new DefaultMQPushConsumer(consumerGroup);
		if(null != consumer){			//consumer创建成功
			//consumer.setNamesrvAddr(namesrv);
			for(String topic:topicSet){
				consumer.subscribe(topic, tag);
			}
			consumer.setConsumeMessageBatchMaxSize(rocketmqConsumerConf.getPullBatchSize() > 0?rocketmqConsumerConf.getPullBatchSize():1);
			consumer.registerMessageListener(listener);
			consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
			consumer.start();
			consumers.put(key, consumer);
		}
		
		return consumer;
	}
}