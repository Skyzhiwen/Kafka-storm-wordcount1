package test;

import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import test.MyKafkaTopology.KafkaWordSplitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public  class KafkaWordSplitter extends BaseRichBolt {

    private static final Log LOG = LogFactory.getLog(KafkaWordSplitter.class);
    private static final long serialVersionUID = 886149197481637894L;
    private OutputCollector collector;
   
    @Override
    public void prepare(Map stormConf, TopologyContext context,
              OutputCollector collector) {
         this.collector = collector;              
    }

    @Override
    public void execute(Tuple input) {
         String line = input.getString(0);
         System.out.println(line);
         LOG.info("RECV[kafka -> splitter] " + line);
         String[] words = line.split("\\s+");
         for(String word : words) {
              LOG.info("EMIT[splitter -> counter] " + word);
              collector.emit(input, new Values(word, 1));
         }
         collector.ack(input);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
         declarer.declare(new Fields("word", "count"));         
    }
   
}
