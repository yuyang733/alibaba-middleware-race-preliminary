package com.alibaba.middleware.race.Tair;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class TairTest {
	private String id = null;
	private Map<String,Object> keyValue = new ConcurrentHashMap<String,Object>();
	
	private static Map<String,TairTest> instances = new ConcurrentHashMap<String, TairTest>();
	
	public static TairTest getInstance(String id){
		if(instances.containsKey(id)){
			return instances.get(id);
		}
		else{
			TairTest instance = new TairTest();
			instance.id = id;
			instances.put(id, instance);
			return instances.get(id);
		}
	}
	
	public boolean write(String key, Object value) {
		this.keyValue.put(key, value);
		return true;
	}

	public Object get(String key) {
		return this.keyValue.get(key);
	}

	public boolean remove(String key) {
		return this.keyValue.remove(key) != null;
	}

	public void close() {
		PrintStream ps = null;
		try {
			ps = new PrintStream( new FileOutputStream("/home/alibaba/tmp/tair/" + this.id + ".out"),true);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		
		for(String key:this.keyValue.keySet()){
			ps.println(key + ":" + this.keyValue.get(key));
		}
		
		ps.close();
	}
	
}
