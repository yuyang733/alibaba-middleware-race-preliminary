package com.alibaba.middleware.race.jstorm.bolt.Tmall;

import java.util.Map;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.model.PaymentMessage;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * 
 * @author yuyang
 *
 */
public class RaceTmallTradeBolt implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -551155844908491762L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wholeTimestamp",RaceConfig.MqPayTopic + "@" + RaceConfig.MqTmallTradeTopic));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {
		PaymentMessage tmallPaymentMsg = (PaymentMessage) input.getValueByField(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTmallTradeTopic);
		
		Long wholeTimestamp = RaceUtils.convertWholePointStamp(tmallPaymentMsg.getCreateTime());
		collector.emit(new Values(wholeTimestamp,tmallPaymentMsg));
		
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}}
