package com.alibaba.middleware.race.jstorm;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.RacePaymentPlatformRatio;
import com.alibaba.middleware.race.jstorm.bolt.TB.RaceTBTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.TB.RaceTBTradeCount;
import com.alibaba.middleware.race.jstorm.bolt.Tmall.RaceTmallTradeBolt;
import com.alibaba.middleware.race.jstorm.bolt.Tmall.RaceTmallTradeCount;
import com.alibaba.middleware.race.jstorm.spout.RaceTradeSpout;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * 这是一个很简单的例子
 * 选手的拓扑提交到集群，我们是有超时设置的。每个选手的拓扑最多跑20分钟，一旦超过这个时间
 * 我们会将选手拓扑杀掉。
 */

/**
 * 选手拓扑入口类，我们定义必须是com.alibaba.middleware.race.jstorm.RaceTopology
 * 因为我们后台对选手的git进行下载打包，拓扑运行的入口类默认是com.alibaba.middleware.race.jstorm.RaceTopology；
 * 所以这个主类路径一定要正确
 */

/**
 * 
 * @author yuyang
 *
 */
public class RaceTopology {

    private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

    public static void main(String[] args) throws Exception {
    	Config conf = new Config();
           int spout_Parallelism_hint = 1;
           int paymentPlatform_Parallelism_hint = 1;
           int TB_Parallelism_hint = 4;
           int TB_COUNT_Parallelism_hint = 4;
           int Tmall_Parallelism_hint = 4;
           int Tmall_COUNT_Parallelism_hint = 4;

           TopologyBuilder builder = new TopologyBuilder();

           builder.setSpout("RaceTradeSpout", new RaceTradeSpout(), spout_Parallelism_hint);
           
           builder.setBolt("RaceTBTradeBolt", new RaceTBTradeBolt(),TB_Parallelism_hint)
           .shuffleGrouping("RaceTradeSpout", JstormConf.taobaoPaymentStreamId);
           
           builder.setBolt("RaceTmallTradeBolt", new RaceTmallTradeBolt(),Tmall_Parallelism_hint)
           .shuffleGrouping("RaceTradeSpout",JstormConf.tmallPaymentStreamId);
           
           builder.setBolt("RaceTBTradeCount", new RaceTBTradeCount(),TB_COUNT_Parallelism_hint)
           .fieldsGrouping("RaceTBTradeBolt", new Fields("wholeTimestamp"));
           
           builder.setBolt("RaceTmallTradeCount", new RaceTmallTradeCount(),Tmall_COUNT_Parallelism_hint)
           .fieldsGrouping("RaceTmallTradeBolt", new Fields("wholeTimestamp"));
           
           builder.setBolt("RacePaymentPlatformRatio", new RacePaymentPlatformRatio(),paymentPlatform_Parallelism_hint)
           .globalGrouping("RaceTBTradeCount")
           .globalGrouping("RaceTmallTradeCount");
           
           String topologyName = RaceConfig.JstormTopologyName;
           Utils.sleep(10);
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());       
	     /* if(args!=null && args.length > 0) {
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
	     } else {
			try {
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology(topologyName, conf, builder.createTopology());
				Thread.sleep(20 * 60 * 1000);
				cluster.killTopology(topologyName);
				cluster.shutdown();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}*/
	}
}