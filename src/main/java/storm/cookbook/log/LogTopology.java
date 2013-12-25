package storm.cookbook.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.contrib.cassandra.bolt.CassandraCounterBatchingBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.utils.Utils;

/**
 * User: domenicosolazzo
 */
public class LogTopology {
    private TopologyBuilder builder = new TopologyBuilder();
    private Config conf = new Config();
    private LocalCluster cluster;

    public LogTopology(){

        builder.setSpout("logSpout", new Logspout(), 10);

        builder.setBolt("logRules", new LogRulesBolt(), 10)
                .shuffleGrouping("logSpout");
        builder.setBolt("indexer", new IndexerBolt(), 10)
                .shuffleGrouping("logRules");
        builder.setBolt("counter", new VolumeCountingBolt(), 10)
                .shuffleGrouping("logRules");
        CassandraCounterBatchingBolt logPersistenceBolt = new  CassandraCounterBatchingBolt(
                Conf.COUNT_CF_NAME, VolumeCountingBolt.FIELD_ROW_KEY, VolumeCountingBolt.FIELD_INCREMENT);
        builder.setBolt("countPersistor", logPersistenceBolt, 10)
                .shuffleGrouping("counter");

        conf.put(Conf.REDIS_PORT_KEY, Conf.DEFAULT_JEDIS_PORT);
        conf.put(CassandraBolt.CASSANDRA_KEYSPACE, Conf.LOGGING_KEYSPACE);

    }

    public TopologyBuilder getBuilder(){
        return builder;
    }

    public Config getConfig(){
        return conf;
    }
    public LocalCluster getLocalCluster(){
        return cluster;
    }

    public void runLocal(int runTime) {
        conf.setDebug(true);
        conf.put(Conf.REDIS_HOST_KEY, "localhost");
        conf.put(CassandraBolt.CASSANDRA_HOST, "localhost:9171");
        cluster = new LocalCluster();
        cluster.submitTopology("test", conf, builder.createTopology());
        if (runTime > 0) {
            Utils.sleep(runTime);
            shutDownLocal();
        }
    }

    public void shutDownLocal() {
        if (cluster != null) {
            cluster.killTopology("test");
            cluster.shutdown();
        }
    }

}
