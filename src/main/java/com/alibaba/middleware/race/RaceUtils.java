package com.alibaba.middleware.race;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import org.yaml.snakeyaml.Yaml;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;


public class RaceUtils {
    /**
     * 由于我们是将消息进行Kryo序列化后，堆积到RocketMq，所有选手需要从metaQ获取消息，
     * 反序列出消息模型，只要消息模型的定义类似于OrderMessage和PaymentMessage即可
     * @param object
     * @return
     */
    public static byte[] writeKryoObject(Object object) {
        Output output = new Output(1024);
        Kryo kryo = new Kryo();
        kryo.writeObject(output, object);
        output.flush();
        output.close();
        byte [] ret = output.toBytes();
        output.clear();
        return ret;
    }

    public static <T> T readKryoObject(Class<T> tClass, byte[] bytes) {
        Kryo kryo = new Kryo();
        Input input = new Input(bytes);
        input.close();
        T ret = kryo.readObject(input, tClass);
        return ret;
    }
        
    public static String convertTime(long mill){	//时间戳的转换
    	SimpleDateFormat sdf = new SimpleDateFormat("yyyy-mm-dd hh:mm:ss");
    	String strs  = sdf.format(new Date(Long.parseLong(String.valueOf(mill))));
		return strs;	
    }
    
    public static long convertWholePointStamp(long mill){
    	return (mill/1000/60)*60;
    }
    
    public static double roundDouble(double f, int n){
    	System.out.println("f" +":" + String.valueOf(f));
    	BigDecimal  b = new BigDecimal(f);
    	return b.setScale(n, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
