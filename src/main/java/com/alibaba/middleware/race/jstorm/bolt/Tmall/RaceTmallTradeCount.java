package com.alibaba.middleware.race.jstorm.bolt.Tmall;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.GCHelper;
import com.alibaba.middleware.race.jstorm.PersistenceHelper;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RaceTmallTradeCount implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4004030007073939726L;
	private Map<Long,Double> tmallCountList = new ConcurrentHashMap<Long,Double>();
	private Queue<PaymentMessage> processedList = new ConcurrentLinkedQueue<PaymentMessage>();
	private transient ExecutorService threadPool = null;
	private transient TairOperatorImpl tairClient = null;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(RaceConfig.MqPayTopic));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.tairClient = TairOperatorImpl.getInstance();
		threadPool = Executors.newCachedThreadPool();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		PaymentMessage tmallPaymentMsg = (PaymentMessage) input.getValueByField(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTmallTradeTopic);
		
		if(this.processedList.contains(tmallPaymentMsg)){
			return;
		}
		
		Long wholeTimestamp =  RaceUtils.convertWholePointStamp(tmallPaymentMsg.getCreateTime());
		
		if(this.tmallCountList.containsKey(wholeTimestamp)){
			this.tmallCountList.put(wholeTimestamp, this.tmallCountList.get(wholeTimestamp) + tmallPaymentMsg.getPayAmount());
		}
		else{
			this.tmallCountList.put(wholeTimestamp, tmallPaymentMsg.getPayAmount());
		}
		collector.emit(new Values(tmallPaymentMsg));
		this.processedList.add(tmallPaymentMsg);
		
		this.tairClient.write(RaceConfig.prex_tmall + RaceConfig.teamcode + wholeTimestamp, this.tmallCountList.get(wholeTimestamp));
		
		if(this.threadPool != null){
//			this.threadPool.submit(new PersistenceHelper(RaceConfig.prex_tmall + RaceConfig.teamcode, this.tmallCountList));
			this.threadPool.submit(new GCHelper<>(this.processedList	, 10000));
		}
	}

	@Override
	public void cleanup() {
		if(this.tairClient != null){
			this.tairClient.close();
		}
		if(this.threadPool != null){
			this.threadPool.shutdown();
		}
	}

}
