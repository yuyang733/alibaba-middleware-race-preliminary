package com.alibaba.middleware.race.rocketmq;

import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;

/**
 * 
 * @author keehang
 *
 */

public class RocketMQConsumerConfig {
	
	//字段名
	public static final String TOPIC_FIELD = "rocketmq.topic";
	public static final String NAMESERVER_FILED = "rocketmq.nameserver";
	public static final String GROUP_FILED = "rocketmq.group";
	public static final String TAGS_FILED = "message.tag";
	
	private  Set<String> topicSet = new ConcurrentSkipListSet<String>();					//允许订阅多个topic
	private  String namesrv = "192.168.1.1:9876";
	private  String group ;
	private  String tag;
	private 	int 	  PullBatchSize = 0 ;
	private   	int	  PullThreadNumMax = 0;
	private 	int 	  PullThreadNumMin = 0;
	
	public int getPullThreadNumMax() {
		return PullThreadNumMax;
	}

	public void setPullThreadNumMax(int pullThreadNumMax) {
		PullThreadNumMax = pullThreadNumMax;
	}

	public int getPullThreadNumMin() {
		return PullThreadNumMin;
	}

	public void setPullThreadNumMin(int pullThreadNumMin) {
		PullThreadNumMin = pullThreadNumMin;
	}

	public int getPullInterval() {
		return PullInterval;
	}

	public void setPullInterval(int pullInterval) {
		PullInterval = pullInterval;
	}

	private  	int	  PullInterval;
	
	
	public RocketMQConsumerConfig(){
	}
	
	public void setTopic(String topic) {
		this.topicSet.add(topic);
	}

	public void setNamesrv(String namesrv) {
		this.namesrv = namesrv;
	}

	public void setGroup(String group) {
		this.group = group;
	}

	public void setTag(String tag) {
		this.tag = tag;
	}

	public int getPullBatchSize() {
		return PullBatchSize;
	}

	public void setPullBatchSize(int pullBatchSize) {
		PullBatchSize = pullBatchSize;
	}
	
	public Set<String> getTopic() {
		return this.topicSet;
	}
	public String getNamesrv() {
		return namesrv;
	}
	public String getGroup() {
		return group;
	}

	public String getTag() {
		return tag;
	}
	
}
