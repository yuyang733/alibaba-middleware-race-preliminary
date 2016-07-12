/**
 * 
 */
package com.alibaba.middleware.race.Tair;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.management.RuntimeErrorException;

import com.alibaba.middleware.race.RaceConfig;
import com.taobao.tair.DataEntry;
import com.taobao.tair.Result;
import com.taobao.tair.ResultCode;
import com.taobao.tair.impl.DefaultTairManager;

/**
 * @author yuyang
 *
 */
public class TairOperatorImpl{
	
	private DefaultTairManager tairManager = new DefaultTairManager();
	private List<String> masterConfigServerList =  new ArrayList<String>();
	private int 			namespace;
	private AtomicInteger referenceCount;
	
	private static TairOperatorImpl instance= null;
	
	public static TairOperatorImpl getInstance(){
		if(instance == null){
			synchronized(TairOperatorImpl.class){
				if(null == instance){
					instance = new TairOperatorImpl(RaceConfig.TairConfigServer, RaceConfig.TairSalveConfigServer, RaceConfig.TairGroup, RaceConfig.TairNamespace);
				}
			}
		}
		instance.referenceCount.incrementAndGet();
		return instance;
	}
	
	public TairOperatorImpl(String masterConfigServer, String slaveConfigServer, String groupName, int namespace) {
		if(masterConfigServer != null && !masterConfigServer.isEmpty() && !this.masterConfigServerList.contains(masterConfigServer)){
			this.masterConfigServerList.add(masterConfigServer);
			if(slaveConfigServer != null && !slaveConfigServer.isEmpty() && !this.masterConfigServerList.contains(slaveConfigServer)){
				this.masterConfigServerList.add(slaveConfigServer);
			}
		}
		this.namespace = namespace;
		
		this.tairManager.setConfigServerList(this.masterConfigServerList);
		this.tairManager.setGroupName(groupName);
		this.tairManager.init();		
		this.referenceCount = new AtomicInteger(0);
	}

	public boolean write(Serializable key, Serializable value) {
		ResultCode rc = tairManager.put(namespace, key, value);
		return rc.isSuccess();
	}

	public Object get(Serializable key) {
		Result<DataEntry> result = tairManager.get(namespace, key);
		if(result.isSuccess()){
			DataEntry entry = result.getValue();
			if(entry != null){
				//数据存在
				return entry.getValue();
			}
			else{
				return null;
			}
		}
		else{
			throw new RuntimeException("The get method given by tair occurs exception!, ErrorInfo :" + result.getRc().getMessage());
		}
	}

	public boolean remove(Serializable key) {
		ResultCode rc = tairManager.delete(namespace, key);
		return rc.isSuccess();
	}

	
	public void close() {
		if(this.referenceCount.decrementAndGet() <= 0){
			tairManager.close();
			this.tairManager = null;
			this.masterConfigServerList.clear();
			instance = null;
		}
	}

	// 天猫的分钟交易额写入tair
	public static void main(String[] args) throws Exception {
		TairOperatorImpl tairclient1 = TairOperatorImpl.getInstance();
		String prefix = RaceConfig.teamcode;
		for(int i = 0; i != 100; i++){
			if(tairclient1.write(prefix+i, i)){
				System.out.println("写入成功!");
			}
			else{
				throw new RuntimeException("写入异常!");
			}
		}
		tairclient1.close();
		Thread.sleep(10000);
		
		TairOperatorImpl tairclient2 = TairOperatorImpl.getInstance();
		for(int i = 0; i != 100; i++){
			Integer value =  (Integer) tairclient2.get(prefix+i);
			if(null != value){
				System.out.println(value);
			}
			else{
				System.out.println("读取失败!");
			}
		}
		tairclient2.close();
	}
}
