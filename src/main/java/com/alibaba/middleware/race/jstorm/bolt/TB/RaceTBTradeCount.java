package com.alibaba.middleware.race.jstorm.bolt.TB;

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

public class RaceTBTradeCount implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = -1304637065097627963L;
	
	private Map<Long,Double> taobaoCountList = new ConcurrentHashMap<Long,Double>();
	private Queue<PaymentMessage> processedList = new ConcurrentLinkedQueue<PaymentMessage>();
	private transient ExecutorService threadPool =null;
	private transient TairOperatorImpl tairClient = null;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields(RaceConfig.MqPayTopic));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.tairClient = TairOperatorImpl.getInstance();
		this.threadPool = Executors.newCachedThreadPool();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		PaymentMessage taobaoPayMsg = (PaymentMessage) input.getValueByField(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTaobaoTradeTopic);
		if(this.processedList.contains(taobaoPayMsg)){
			return;					//舍弃不要
		}
		
		Long wholeTimestamp = RaceUtils.convertWholePointStamp(taobaoPayMsg.getCreateTime());
		if(this.taobaoCountList.containsKey(wholeTimestamp)){
			this.taobaoCountList.put(wholeTimestamp, this.taobaoCountList.get(wholeTimestamp) + taobaoPayMsg.getPayAmount());
		}
		else{
			this.taobaoCountList.put(wholeTimestamp, taobaoPayMsg.getPayAmount());
		}
		collector.emit(new Values(taobaoPayMsg));
		this.processedList.add(taobaoPayMsg);
		
		this.tairClient.write(RaceConfig.prex_taobao + RaceConfig.teamcode + wholeTimestamp, this.taobaoCountList.get(wholeTimestamp));
		
		if(this.threadPool != null){
//			this.threadPool.submit(new PersistenceHelper(RaceConfig.prex_taobao + RaceConfig.teamcode, this.taobaoCountList));
			this.threadPool.submit(new GCHelper<>(this.processedList, 10000));
		}
	}

	@Override
	public void cleanup() {
//		
		if(null != this.tairClient){
			this.tairClient.close();
		}
		if(this.threadPool != null){
			this.threadPool.shutdown();
		}
	}

}
