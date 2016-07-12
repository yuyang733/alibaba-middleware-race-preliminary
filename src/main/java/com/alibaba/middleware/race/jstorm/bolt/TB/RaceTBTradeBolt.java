package com.alibaba.middleware.race.jstorm.bolt.TB;

import java.beans.PersistenceDelegate;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.math.BigDecimal;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.GCHelper;
import com.alibaba.middleware.race.jstorm.JstormConf;
import com.alibaba.middleware.race.jstorm.PersistenceHelper;
import com.alibaba.middleware.race.jstorm.spout.RaceTradeSpout;
import com.alibaba.middleware.race.model.OrderMessage;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import clojure.lang.PersistentArrayMap;

/**
 * 
 * @author yuyang
 *
 */

public class RaceTBTradeBolt implements IBasicBolt {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1244477104327162104L;

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("wholeTimestamp",RaceConfig.MqPayTopic + "@" + RaceConfig.MqTaobaoTradeTopic));
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
		PaymentMessage taobaoPaymentMsg = (PaymentMessage) input.getValueByField(RaceConfig.MqPayTopic + "@" + RaceConfig.MqTaobaoTradeTopic);
		
		Long wholeTimestamp = RaceUtils.convertWholePointStamp(taobaoPaymentMsg.getCreateTime());
		collector.emit(new Values(wholeTimestamp,taobaoPaymentMsg));
	}

	@Override
	public void cleanup() {
		// TODO Auto-generated method stub
		
	}
	
}

