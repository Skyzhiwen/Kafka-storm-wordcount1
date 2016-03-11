//�ù��̣���kafka��storm��һ�����ϳɹ��İ�����storm ��kafka��topicΪkafkatopic��
//�н�ȡ���ݣ�֮����м򵥵�wordcount ����  
// time: 3.9 2016    author:  zhiwen 

package test;

import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import com.google.common.collect.ImmutableList;


import storm.kafka.BrokerHosts;
import storm.kafka.KafkaSpout;
import storm.kafka.SpoutConfig;
import storm.kafka.StringScheme;
import storm.kafka.ZkHosts;
import storm.kafka.bolt.KafkaBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
//import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
//import backtype.storm.generated.Nimbus;
import backtype.storm.spout.SchemeAsMultiScheme;
//import backtype.storm.task.OutputCollector;
//import backtype.storm.task.TopologyContext;
//import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.KafkaConfig;


public class MyKafkaTopology {

     public static void main(String[ ]  args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException {
          String zks = "172.31.101.112:2181";
         String topic = "kafkatopic";
        String zkRoot = "/storm"; // default zookeeper root configuration for storm
          String id = "word";

          BrokerHosts brokerHosts = new ZkHosts(zks);
        //  SpoutConfig spoutConf = new SpoutConfig(brokerHosts, "KafkaTopicOne", zkRoot, id);
          SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot,id);
          spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
          spoutConf.zkServers = Arrays.asList(new String[] {"172.31.101.112"});

          spoutConf.zkPort = 2181;
         
          TopologyBuilder builder = new TopologyBuilder();
          builder.setSpout("kafka-reader", new KafkaSpout(spoutConf), 1); // Kafka���Ǵ�����һ��5������Topic�����ﲢ�ж�����Ϊ5
          builder.setBolt("word-splitter", new KafkaWordSplitter(), 1).shuffleGrouping("kafka-reader");
          builder.setBolt("word-counter", new WordCounter()).fieldsGrouping("word-splitter", new Fields("word"));
          
          Config conf = new Config();
          conf.setDebug(true);
          
         String name = MyKafkaTopology.class.getSimpleName();

          if (args != null && args.length > 0) 
          {
              //  Nimbus host name passed from command line
               conf.put(Config.NIMBUS_HOST, "172.31.101.112");
               conf.setNumWorkers(2);
               StormSubmitter.submitTopology(name, conf, builder.createTopology());
           } 
          else 
           {
        	 
//               conf.setMaxTaskParallelism(3);
               LocalCluster cluster = new LocalCluster();
               cluster.submitTopology("kafka_storm", conf, builder.createTopology());
//               Thread.sleep(100000);
//               cluster.shutdown();
          }
  
}
}
