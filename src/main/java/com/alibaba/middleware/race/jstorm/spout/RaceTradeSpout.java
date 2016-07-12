/**
 * 
 */
package com.alibaba.middleware.race.jstorm.spout;

import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.client.spout.IAckValueSpout;
import com.alibaba.jstorm.client.spout.IFailValueSpout;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.jstorm.JstormConf;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.middleware.race.rocketmq.ConsumerFactory;
import com.alibaba.middleware.race.rocketmq.RocketMQConsumerConfig;
import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.message.MessageExt;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author yuyang
 *
 */
public class RaceTradeSpout implements IRichSpout, MessageListenerOrderly {
	private static final Logger LOG = LoggerFactory.getLogger(RaceTradeSpout.class);
	/**
	 * 
	 */
	private static final long serialVersionUID = 4875048351947598749L;

	private transient DefaultMQPushConsumer consumer; // 这个不需要使用序列化

	private String id;
	private SpoutOutputCollector collector;
	private Map conf; // 配置选项

	//////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
	private Map<Long,String> OrderList = new ConcurrentHashMap<Long,String>();
	private BlockingQueue<PaymentMessage> paymentMsgList = new LinkedBlockingQueue<PaymentMessage>();

	private class SendingMessage {
		PaymentMessage msg = null;
		String streamId = null;
	}

	private Map<UUID, SendingMessage> sendingMsgList = new ConcurrentHashMap<UUID, SendingMessage>();

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		this.conf = conf;
		this.collector = collector;
		this.id = context.getThisComponentId() + ":" + context.getThisTaskId();

		RocketMQConsumerConfig rocketConf = new RocketMQConsumerConfig();
		rocketConf.setNamesrv("192.168.1.1:9876");
		rocketConf.setTopic(RaceConfig.MqTaobaoTradeTopic);
		rocketConf.setTopic(RaceConfig.MqTmallTradeTopic);
		rocketConf.setTopic(RaceConfig.MqPayTopic);
		rocketConf.setTag(null);
		rocketConf.setGroup(RaceConfig.MetaConsumerGroup);
		rocketConf.setPullBatchSize(32);

		try {
			DefaultMQPushConsumer mqConsumer = ConsumerFactory.getConsumer(rocketConf, this);
			if (null != mqConsumer) {
				this.consumer = mqConsumer;
			}
		} catch (MQClientException e) {
			e.printStackTrace();
		}
	}

	public void close() {
		if (this.consumer != null)
			this.consumer.shutdown();
	}

	public void activate() {
		if (this.consumer != null)
			this.consumer.resume();
	}

	public void deactivate() {
		if (this.consumer != null) {
			this.consumer.suspend();
		}
	}

	public void nextTuple() {
		// 发送到bolt当中进行处理
		// 这里需要进行消息分流
		Utils.sleep(10);
		
		PaymentMessage paymentMsg = null;
		while(!this.paymentMsgList.isEmpty()){
			paymentMsg = this.paymentMsgList.poll();
			if(this.OrderList.containsKey(paymentMsg.getOrderId())){
				break;
			}
			else{
				this.paymentMsgList.add(paymentMsg);			//放回原队列
				continue;
			}
		}
		
		if (null != paymentMsg) {
			UUID msgId = UUID.randomUUID();
			SendingMessage sendingMsg = new SendingMessage();
			sendingMsg.msg = paymentMsg;
			
			String paySource = this.OrderList.get(paymentMsg.getOrderId());
			if(paySource.equals("taobao")){
				sendingMsg.streamId = JstormConf.taobaoPaymentStreamId;
			}
			
			if(paySource.equals("tmall")){
				sendingMsg.streamId = JstormConf.tmallPaymentStreamId;
			}
			
			this.collector.emit(sendingMsg.streamId, new Values(sendingMsg.msg),msgId);
			
			this.sendingMsgList.put(msgId, sendingMsg);
			
		}
	}
	
	@Override
	public void ack(Object msgId) {
		if (this.sendingMsgList.containsKey(msgId)) {
			this.sendingMsgList.remove(msgId); // 直接从队列中移除
		}
	}

	@Override
	public void fail(Object msgId) {
		if(this.sendingMsgList.containsKey(msgId)){
			SendingMessage sendingMsg = this.sendingMsgList.get(msgId);
			this.collector.emit(sendingMsg.streamId,new Values(sendingMsg.msg));				//重新发送这条消息
			LOG.warn("正在重发消息:" + sendingMsg.msg.toString());
		}
		else{
			LOG.error("确认了一条不存在的MsgId");
		}
	}
	
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declareStream(JstormConf.taobaoPaymentStreamId, new Fields(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTaobaoTradeTopic));
		declarer.declareStream(JstormConf.paymentStreamId, new Fields(RaceConfig.MqPayTopic));
		declarer.declareStream(JstormConf.tmallPaymentStreamId, new Fields(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTmallTradeTopic));
	}

	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
		context.setAutoCommit(true); // 设置自动更新
		for (MessageExt msg : msgs) {
			byte[] body = msg.getBody();
			if (body.length == 2 && body[0] == 0 && body[1] == 0) {
				// Info: 生产者停止生成数据, 并不意味着马上结束
				continue;
			}
			if (msg.getTopic().equals(RaceConfig.MqTaobaoTradeTopic)) {
				OrderMessage taobaoOrderMsg = RaceUtils.readKryoObject(OrderMessage.class, body);
				this.OrderList.put(taobaoOrderMsg.getOrderId(), "taobao");
			}

			if (msg.getTopic().equals(RaceConfig.MqTmallTradeTopic)) {
				OrderMessage tmallOrderMsg = RaceUtils.readKryoObject(OrderMessage.class, body);
				this.OrderList.put(tmallOrderMsg.getOrderId(), "tmall");
			}

			if (msg.getTopic().equals(RaceConfig.MqPayTopic)) {
				PaymentMessage paymentMsg = RaceUtils.readKryoObject(PaymentMessage.class, body);
				try {
					if (this.paymentMsgList.offer(paymentMsg, 10, TimeUnit.MILLISECONDS)) {
						continue;
					} else {
						return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
					}
				} catch (InterruptedException e) {
					LOG.error(e.getMessage());
					return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
				}
			}
		}
		return ConsumeOrderlyStatus.SUCCESS;
	}
}