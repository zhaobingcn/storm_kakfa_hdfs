package com.learn.storm_kafka_hdfs;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.AuthorizationException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.hdfs.bolt.HdfsBolt;
import org.apache.storm.hdfs.bolt.format.DefaultFileNameFormat;
import org.apache.storm.hdfs.bolt.format.DelimitedRecordFormat;
import org.apache.storm.hdfs.bolt.format.FileNameFormat;
import org.apache.storm.hdfs.bolt.format.RecordFormat;
import org.apache.storm.hdfs.bolt.rotation.FileRotationPolicy;
import org.apache.storm.hdfs.bolt.rotation.TimedRotationPolicy;
import org.apache.storm.hdfs.bolt.sync.CountSyncPolicy;
import org.apache.storm.hdfs.bolt.sync.SyncPolicy;
import storm.kafka.*;

import java.util.Arrays;
import java.util.Map;

/**
 *Storm hdfs kafka整合
 */
public class DistributeWordTopology
{
    public static class KafkaWordToUpperCase extends BaseRichBolt {

        private static final Log LOG = LogFactory.getLog(KafkaWordToUpperCase.class);
        private static final long serialVersionUID = -5207232012035109026L;
        private OutputCollector collector;

        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.collector = outputCollector;
        }

        public void execute(Tuple tuple) {
            String line = tuple.getString(0).trim();
            LOG.info("RECV[kafka -> splitter]" + line);
            if(!line.isEmpty()){
                String upperLine = line.toUpperCase();
                LOG.info("EMIT[splitter -> counter]" + upperLine);
                collector.emit(tuple, new Values(upperLine, upperLine.length()));
            }
            collector.ack(tuple);
        }

        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields("line", "len"));
        }
    }

    public static class RealtimeBolt extends BaseRichBolt{

        private static final Log LOG = LogFactory.getLog(KafkaWordToUpperCase.class);
        private static final long serialVersionUID = -4115132557403913367L;
        private OutputCollector outputCollector;

        @Override
        public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
            this.outputCollector = outputCollector;
        }

        @Override
        public void execute(Tuple tuple) {
            String line = tuple.getString(0).trim();
            LOG.info("REALTIME :" + line);
            outputCollector.ack(tuple);
        }

        @Override
        public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

        }
    }

    public static void main(String[] args) throws AlreadyAliveException, InvalidTopologyException, InterruptedException, AuthorizationException{

        String zks = "ubuntu-01:2181,ubuntu-02:2181,ubuntu-03:2181";
        String topic = "my-replic";
        String zkRoot = "/storm";
        String id = "word";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConfig = new SpoutConfig(brokerHosts, topic, zkRoot, id);
        spoutConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConfig.zkServers = Arrays.asList(new String[] {"ubuntu-01", "ubuntu-02", "ubuntu-03"});
        spoutConfig.zkPort = 2181;

        RecordFormat recordFormat = new DelimitedRecordFormat().withRecordDelimiter("\t");

        SyncPolicy syncPolicy = new CountSyncPolicy(1000);

        FileRotationPolicy rotationPolicy = new TimedRotationPolicy(1.0f, TimedRotationPolicy.TimeUnit.MINUTES);

        FileNameFormat fileNameFormat = new DefaultFileNameFormat().withPath("/storm/").withPrefix("app_").withExtension(".log");

        HdfsBolt hdfsBolt = new HdfsBolt()
                .withFsUrl("hdfs://ubuntu-01:9000")
                .withFileNameFormat(fileNameFormat)
                .withRecordFormat(recordFormat)
                .withRotationPolicy(rotationPolicy)
                .withSyncPolicy(syncPolicy);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("kafka-reader", new KafkaSpout(spoutConfig), 5);
        builder.setBolt("to-upper", new KafkaWordToUpperCase(), 3).shuffleGrouping("kafka-reader");
        builder.setBolt("hdfs-bolt", hdfsBolt, 2).shuffleGrouping("to-upper");
        builder.setBolt("realtime", new RealtimeBolt(), 2).shuffleGrouping("to-upper");

        Config config = new Config();
        String name = DistributeWordTopology.class.getSimpleName();
        if(args != null && args.length > 0){
            String nimbus = args[0];
            config.put(Config.NIMBUS_HOST, nimbus);
            config.setNumWorkers(3);
            StormSubmitter.submitTopologyWithProgressBar(name, config, builder.createTopology());
        }else{
            config.setMaxTaskParallelism(3);
            LocalCluster localCluster = new LocalCluster();
            localCluster.submitTopology(name, config, builder.createTopology());
            Thread.sleep(60000);
            localCluster.shutdown();
        }

    }
}
