package storm.cookbook.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.drools.KnowledgeBase;
import org.drools.KnowledgeBaseFactory;
import org.drools.builder.KnowledgeBuilder;
import org.drools.builder.KnowledgeBuilderFactory;
import org.drools.builder.ResourceType;
import org.drools.io.ResourceFactory;
import org.drools.runtime.StatelessKnowledgeSession;
import storm.cookbook.log.model.LogEntry;

import java.util.Map;
import java.util.logging.Logger;

/**
 * User: domenicosolazzo
 */
public class LogRulesBolt extends BaseRichBolt{

    public static Logger LOG = Logger.getLogger(LogRulesBolt.class.toString());
    private StatelessKnowledgeSession ksession;
    private OutputCollector collector;
    @Override
    public void prepare(Map conf, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = collector;

        KnowledgeBuilder kbuilder = new KnowledgeBuilderFactory().newKnowledgeBuilder();
        kbuilder.add( ResourceFactory.newClassPathResource( "/Syslog.drl",
                getClass() ), ResourceType.DRL );
        if( kbuilder.hasErrors() ){
            LOG.error(kbuilder.getErrors().toString());

        }
        KnowledgeBase knowledgeBase = KnowledgeBaseFactory.newKnowledgeBase();
        knowledgeBase.addKnowledgePackages(kbuilder.getKnowledgePackages());
        ksession = knowledgeBase.newStatelessKnowledgeSession();
    }

    @Override
    public void execute(Tuple tuple) {
        LogEntry entry = (LogEntry) tuple.getValueByField(FieldNames.LOG_ENTRY);
        if( entry == null ){
            LOG.fatal("Received null or invalid value from the tuple");
            return;

        }
        ksession.execute( entry );

        if ( !entry.isFilter() ){
            LOG.debug("Emitting the Rules Bolt!");
            this.collector.emit(new Values(entry));
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FieldNames.LOG_ENTRY));
    }
}
