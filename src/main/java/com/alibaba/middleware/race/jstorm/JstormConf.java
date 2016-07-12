package com.alibaba.middleware.race.jstorm;

import com.alibaba.middleware.race.RaceConfig;

/**
 * 
 * @author yuyang
 *
 */
public class JstormConf {
	public static int consumeMessageBatchMaxSize = 3;			//批量消费，一次消费多少条消息
	public static int pullBatchSize = 32;												//批量啦消息,一次最多拉多少条
	
	public static String prex_stream = "Stream_";
	public static String taobaoStreamId = prex_stream + RaceConfig.MqTaobaoTradeTopic;
	public static String tmallStreamId 	  = prex_stream + RaceConfig.MqTmallTradeTopic;
	
	public static String paymentStreamId = prex_stream + RaceConfig.MqPayTopic;
	public static String taobaoPaymentStreamId = paymentStreamId + "_" + RaceConfig.MqTaobaoTradeTopic;
	public static String tmallPaymentStreamId = paymentStreamId + "_"+ RaceConfig.MqTmallTradeTopic;
}
