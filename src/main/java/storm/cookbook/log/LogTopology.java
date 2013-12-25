package storm.cookbook.log;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.cassandra.bolt.CassandraBolt;
import backtype.storm.contrib.cassandra.bolt.CassandraCounterBatchingBolt;
import backtype.storm.topology.TopologyBuilder;

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
}
