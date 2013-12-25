package storm.cookbook.log;

import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.apache.log4j.Logger;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import storm.cookbook.log.model.LogEntry;

import java.util.Map;

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
        LogEntry entry = (LogEntry) tuple.getValueByField(FieldNames.LOG_ENTRY);
        if (entry == null){
            LOG.fatal("The entry is null or invalid.");
            return;
        }
        String toBeIndexed = entry.toJSON().toJSONString();
        IndexResponse response = client.prepareIndex(INDEX_NAME, INDEX_TYPE)
                .setSource(toBeIndexed)
                .execute().actionGet();
        if ( response == null ){
            LOG.error("Error indexing the tuple: " + tuple.toString());
        }else if (response.getId() == null){
            LOG.error("Index id is null. Error indexing the tuple: " + tuple.toString());

        }else{
            LOG.debug("The tuple has been indexed. " + tuple.toString());
            this.collector.emit(new Values( entry, response.getId() ));
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.LOG_ENTRY, FieldNames.LOG_INDEX_ID));
    }
}
