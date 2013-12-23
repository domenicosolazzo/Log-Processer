package storm.cookbook.log;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;

import java.util.Map;
import java.util.logging.Logger;

/**
 * User: domenicosolazzo
 */
public class IndexerBolt extends BaseRichBolt{
    // Elastic search client
    private Client client;
    // Logger
    private Logger LOG = Logger.getLogger(IndexerBolt.class.toString());

    // Collector
    private OutputCollector collector;

    private static final String INDEX_NAME = "logstorm";
    private static final String INDEX_TYPE = "logentry";
    @Override
    public void prepare(Map stormConf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;

        Node node;
        if((Boolean)stormConf.get(Config.TOPOLOGY_DEBUG) == true){
            // Topology for local testing
            node = NodeBuilder.nodeBuilder().local(true).node();
        }else{
            String clusterName = (String) stormConf.get(Conf.ELASTIC_CLUSTER_NAME);
            if (clusterName == null){
                clusterName = Conf.DEFAULT_ELASTIC_CLUSTER;
            }
            node = NodeBuilder.nodeBuilder().clusterName(clusterName).node();
        }
        client = node.client();

    }

    @Override
    public void execute(Tuple tuple) {

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
