package com.alibaba.middleware.race.test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;
import java.util.Random;
import java.util.TreeMap;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendCallback;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

public class DataGenerator {	
	private static final int count = 100;
	
	private static PrintWriter OriginData = null;
	private static PrintWriter OriginTaobaoCount = null;
	private static PrintWriter OriginTmallCount = null;
	private static PrintWriter OriginRatio = null;
	
	private static Map<Long,Double> taobaoOrderList = new HashMap<Long,Double>();
	private static Map<Long,Double> tmallOrderList = new HashMap<Long,Double>();
	
//	private static Map<Long, Double[]> ratio = new TreeMap<Long,Double[]>();
	private static Map<Long, Double> taobaoCount = new HashMap<Long,Double>();
	private static Map<Long, Double> tmallCount = new HashMap<Long,Double>();
	
	public static void main(String[] args) throws FileNotFoundException, InterruptedException, MQClientException, RemotingException {
		OriginData = new PrintWriter(new FileOutputStream("/home/alibaba/tmp/tair/OriginData.out"),true);
		OriginTaobaoCount = new PrintWriter(new FileOutputStream("/home/alibaba/tmp/tair/OriginTaobaoCount.out"),true);
		OriginTmallCount = new PrintWriter(new FileOutputStream("/home/alibaba/tmp/tair/OriginTmallCount.out"),true);
		
		DefaultMQProducer producer = new DefaultMQProducer("STALL_GROUP");
        producer.setNamesrvAddr("192.168.1.1:9876");
        producer.start();
        final Semaphore semaphore = new Semaphore(0);
        
		//开始生成数据
		for(int n = 0; n < 3; n++){
			for(int i = 0; i < count; i++){
				Random rand = new Random();
				final int platform = rand.nextInt(2);
				final OrderMessage orderMessage = ( platform == 0 ? OrderMessage.createTbaoMessage() : OrderMessage.createTmallMessage());
	            orderMessage.setCreateTime(System.currentTimeMillis());
	            
	            byte [] body = RaceUtils.writeKryoObject(orderMessage);
                Message msgToBroker = new Message(platform ==0 ? RaceConfig.MqTaobaoTradeTopic:RaceConfig.MqTmallTradeTopic, body);

                producer.send(msgToBroker, new SendCallback() {
                    public void onSuccess(SendResult sendResult) {
                    	 OriginData.println(orderMessage);
         	            if(platform == 0){
        	            	taobaoOrderList.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
        	            }
        	            else{
        	            	tmallOrderList.put(orderMessage.getOrderId(), orderMessage.getTotalPrice());
        	            }
                        semaphore.release();
                    }
                    public void onException(Throwable throwable) {
                        throwable.printStackTrace();
                    }
                });
	            
	            PaymentMessage[] paymentMessages = PaymentMessage.createPayMentMsg(orderMessage);
	            for (final PaymentMessage paymentMessage : paymentMessages) {
	            	
	            	final Message messageToBroker = new Message(RaceConfig.MqPayTopic, RaceUtils.writeKryoObject(paymentMessage));
                    producer.send(messageToBroker, new SendCallback() {
                        public void onSuccess(SendResult sendResult) {
        	            	OriginData.println(paymentMessage.toString() + "     " + RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime()) );
        	            	if(taobaoOrderList.containsKey(paymentMessage.getOrderId())){
        	            		double amountCount = (taobaoCount.get(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime())) == null ? 0:taobaoCount.get(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime())).doubleValue()) + paymentMessage.getPayAmount();
        	            		taobaoCount.put(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime()), amountCount);
        	            		OriginTaobaoCount.println(paymentMessage.toString() + "     " + RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime()) );
        	            	}
        	            	if(tmallOrderList.containsKey(paymentMessage.getOrderId())){
        	            		double amountCount = (tmallCount.get(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime())) ==null?0:tmallCount.get(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime())).doubleValue()) + paymentMessage.getPayAmount();
        	            		tmallCount.put(RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime()), amountCount);
        	            		OriginTmallCount.println(paymentMessage.toString() + "     " + RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime()));
        	            	}
                        }
                        public void onException(Throwable throwable) {
                            throwable.printStackTrace();
                        }
                    });
	            	
//	            	Long timestamp = RaceUtils.convertWholePointStamp(paymentMessage.getCreateTime());
//	            	if(ratio.containsKey(timestamp)){
//	            		Double[]  platformCount= ratio.get(timestamp);
//	            		platformCount[paymentMessage.getPayPlatform()] += paymentMessage.getPayAmount();
//	            		ratio.put(timestamp, platformCount);
//	            	}
//	            	else{
//	            		Double[] platformCount = new Double[2];
//	            		platformCount[0] = (double) 0;
//	            		platformCount[1] = (double) 0;
//	            		platformCount[paymentMessage.getPayPlatform()] = paymentMessage.getPayAmount();
//	            	}
	            }
			}
			Thread.sleep(1000 * 60);
		}
		
        semaphore.acquire(count);

        //用一个short标识生产者停止生产数据
        byte [] zero = new  byte[]{0,0};
        Message endMsgTB = new Message(RaceConfig.MqTaobaoTradeTopic, zero);
        Message endMsgTM = new Message(RaceConfig.MqTmallTradeTopic, zero);
        Message endMsgPay = new Message(RaceConfig.MqPayTopic, zero);
		
		for(Long key: taobaoCount.keySet()){
			OriginTaobaoCount.println(String.valueOf(key.longValue()) + ":" + String.valueOf(taobaoCount.get(key).doubleValue()));
		}
		
		for(Long key: tmallCount.keySet()){
			OriginTmallCount.println(String.valueOf(key.longValue()) + ":" + String.valueOf(tmallCount.get(key).doubleValue()));
		}
		
        try {
            producer.send(endMsgTB);
            producer.send(endMsgTM);
            producer.send(endMsgPay);
        } catch (Exception e) {
            e.printStackTrace();
        }
        producer.shutdown();
        
		
//		for(Long key: ratio.keySet()){
//			Double[] platform = ratio.get(key);
//			double platformRatio = RaceUtils.roundDouble(platform[1]/platform[0], 2);
//			OriginRatio.println(String.valueOf(key.longValue()) + ":" +String.valueOf( platformRatio));
//		}
		
		System.out.println("写入完成!");
		OriginData.close();
		OriginTaobaoCount.close();
		OriginTmallCount.close();
//		OriginRatio.close();
	}

}
