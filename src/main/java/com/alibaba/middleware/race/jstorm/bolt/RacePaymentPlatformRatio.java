package com.alibaba.middleware.race.jstorm.bolt;

import java.beans.PersistenceDelegate;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.RaceUtils;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.jstorm.GCHelper;
import com.alibaba.middleware.race.jstorm.JstormConf;
import com.alibaba.middleware.race.jstorm.PersistenceHelper;
import com.alibaba.middleware.race.model.PaymentMessage;
import com.esotericsoftware.minlog.Log;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.IBasicBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class RacePaymentPlatformRatio implements IBasicBolt {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7650738357986852001L;

	private final long updateMill = 1 * 1000;
	private volatile long lastUpdate = 0;

	private class PaymentPlatform {
		public double[] count;
		public boolean dirty;

		public PaymentPlatform() {
			this.count = new double[2];
			this.count[0] = 0.0;
			this.count[1] = 0.0;
			this.dirty = false;
		}
	}

	private volatile Map<Long, PaymentPlatform> PlatformPaymentAmountWithTimestamp = new TreeMap<Long, PaymentPlatform>(); // 时间戳与比值
	private Map<Long,Double> ratio = new ConcurrentHashMap<Long,Double>();
	private Queue<PaymentMessage> processedList = new ConcurrentLinkedQueue<PaymentMessage>();
	private transient ExecutorService threadPool = null;
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		return null;
	}

	@Override
	public void cleanup() {
		
		if(!this.ratio.isEmpty()){
			PersistenceHelper helper = new PersistenceHelper(RaceConfig.prex_ratio + RaceConfig.teamcode, this.ratio);
			helper.syncData(true);																						//把剩下数据都同步到tair当中
		}
		this.PlatformPaymentAmountWithTimestamp.clear();
		this.processedList.clear();
		if(this.threadPool != null && !this.threadPool.isShutdown())
			this.threadPool.shutdown();
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context) {
		this.threadPool = Executors.newSingleThreadExecutor();
	}

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		try {
			PaymentMessage paymentMsg = (PaymentMessage) input.getValueByField(RaceConfig.MqPayTopic);
			
			if (this.processedList.contains(paymentMsg)) {
				Log.warn("该条支付订单已经处理过,舍弃!");
				return;
			}

			long timeStamp = paymentMsg.getCreateTime();
			if (this.PlatformPaymentAmountWithTimestamp.containsKey(RaceUtils.convertWholePointStamp(timeStamp))) {
				// 加入到现有的整点戳统计中
				PaymentPlatform platformCount = this.PlatformPaymentAmountWithTimestamp
						.get(RaceUtils.convertWholePointStamp(timeStamp));
				platformCount.count[paymentMsg.getPayPlatform()] += paymentMsg.getPayAmount();
				platformCount.dirty = true;
				this.PlatformPaymentAmountWithTimestamp.put(RaceUtils.convertWholePointStamp(timeStamp), platformCount);
				for (Long timestampKey : this.PlatformPaymentAmountWithTimestamp.keySet()) {
					// 更新后面的时间戳
					if (timestampKey > RaceUtils.convertWholePointStamp(timeStamp)) {
						PaymentPlatform tmp = this.PlatformPaymentAmountWithTimestamp.get(timestampKey);
						tmp.count[paymentMsg.getPayPlatform()] += paymentMsg.getPayAmount();
						tmp.dirty = true;
						this.PlatformPaymentAmountWithTimestamp.put(timestampKey, tmp);
					}
				}
			} else {
				// 新建一个新的整点时间戳
				PaymentPlatform platformCount = new PaymentPlatform();
				this.PlatformPaymentAmountWithTimestamp.put(RaceUtils.convertWholePointStamp(timeStamp), platformCount); // 加入到整点时间戳当中

				// 找到它的上一个时间戳
				long lastTimestamp = -1;
				if (this.PlatformPaymentAmountWithTimestamp.keySet().size() > 1) {
					AtomicInteger lastTimestampIndex = new AtomicInteger(0);
					for (Object key : this.PlatformPaymentAmountWithTimestamp.keySet().toArray()) {
						Long timestampKey = (Long) key;
						if (timestampKey.equals(RaceUtils.convertWholePointStamp(timeStamp))) {
							break;
						}
						lastTimestampIndex.incrementAndGet();
					}
					lastTimestamp = lastTimestampIndex.get() > 0 ? (Long) (this.PlatformPaymentAmountWithTimestamp
							.keySet().toArray()[lastTimestampIndex.get() - 1]) : -1;
				}

				platformCount.count[paymentMsg.getPayPlatform()] = ((lastTimestamp != -1)
						? (this.PlatformPaymentAmountWithTimestamp.get(lastTimestamp).count[paymentMsg
								.getPayPlatform()])
						: 0) + paymentMsg.getPayAmount();
				platformCount.dirty = true;
				this.PlatformPaymentAmountWithTimestamp.put(RaceUtils.convertWholePointStamp(timeStamp), platformCount); // 加入到整点时间戳当中

				// 更新后面的时间戳
				for (Long timestampKey : this.PlatformPaymentAmountWithTimestamp.keySet()) {
					if (timestampKey > timeStamp) {
						PaymentPlatform tmp = this.PlatformPaymentAmountWithTimestamp.get(timestampKey);
						tmp.count[paymentMsg.getPayPlatform()] += paymentMsg.getPayAmount();
						tmp.dirty = true;
						this.PlatformPaymentAmountWithTimestamp.put(timestampKey, tmp);
					}
				}
			}


				for (Entry<Long, PaymentPlatform> keyValue : this.PlatformPaymentAmountWithTimestamp.entrySet()) {
					if (keyValue.getValue().dirty && Double.compare(keyValue.getValue().count[0], 0) != 0) {
						double ratio = keyValue.getValue().count[1] / keyValue.getValue().count[0];
						ratio = RaceUtils.roundDouble(ratio, 2);
						this.ratio.put(keyValue.getKey(), ratio);
					}
				}
				// TODO:性能优化点
				
			collector.emit(JstormConf.taobaoPaymentStreamId, new Values(input.getValueByField(RaceConfig.MqPayTopic)));
			collector.emit(JstormConf.tmallPaymentStreamId, new Values(input.getValueByField(RaceConfig.MqPayTopic)));
			this.processedList.add(paymentMsg);
		} finally {
			if(this.threadPool != null){
				this.threadPool.submit(new PersistenceHelper(RaceConfig.prex_ratio + RaceConfig.teamcode, this.ratio));
				this.threadPool.submit(new GCHelper<>(processedList, 10000));
			}
		}
	}

}
