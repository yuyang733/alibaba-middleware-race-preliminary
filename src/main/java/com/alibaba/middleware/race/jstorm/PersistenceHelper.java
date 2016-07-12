package com.alibaba.middleware.race.jstorm;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.Tair.TairOperatorImpl;
import com.alibaba.middleware.race.Tair.TairTest;

public class PersistenceHelper implements Callable<Boolean> {

	private TairOperatorImpl tairClient = null;
	private String keyPrefix = null;
	private Map<Long, Double> dataList = null;

	public PersistenceHelper(String keyPrefix, Map<Long, Double> dataList) {
		this.keyPrefix = keyPrefix;
		this.dataList = dataList;
		this.tairClient = TairOperatorImpl.getInstance();
//		if(keyPrefix.startsWith(RaceConfig.prex_ratio)){
//			this.tairClient = TairTest.getInstance(RaceConfig.prex_ratio);
//		}
//		
//		if(keyPrefix.startsWith(RaceConfig.prex_taobao)){
//			this.tairClient = TairTest.getInstance(RaceConfig.prex_taobao);
//		}
//		
//		if(keyPrefix.startsWith(RaceConfig.prex_tmall)){
//			this.tairClient = TairTest.getInstance(RaceConfig.prex_tmall);
//		}
	}
	
	public Boolean syncData(boolean isSyncAll){
		if (null == this.dataList || null == this.tairClient || null == this.keyPrefix) {
			return false;
		}

		// 为了保证数据有效性，每次同步dataList中的一般进去
		Long[] timestampSet = this.dataList.keySet().toArray(new Long[this.dataList.size()]);
		Arrays.sort(timestampSet);

		int syncLength = isSyncAll ?timestampSet.length: timestampSet.length-2;		
		for (int i = 0; i < syncLength; i++) {						
			String key = keyPrefix + timestampSet[i];
			Double value = (Double) this.tairClient.get(key);
			if (value != null) {
				if (this.dataList.get(timestampSet[i]).compareTo(value) == 0) {
					//this.dataList.remove(timestampSet[i]); 																// 如果本次数据之前的数据一致，就删除
				} else {
					this.tairClient.remove(key);
					this.tairClient.write(key, this.dataList.get(timestampSet[i]));						 // 如果不一致的话，就用新的值写入进去
				}
			} else {
				this.tairClient.write(key, this.dataList.get(timestampSet[i])); 							// 如果tair当中没有东西，就写新的进去
			}
		}

		this.tairClient.close();
		
		return true;
	}

	@Override
	public Boolean call() throws Exception {
		return this.syncData(true);
	}
}
