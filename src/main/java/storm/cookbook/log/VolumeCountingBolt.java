package storm.cookbook.log;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import storm.cookbook.log.model.LogEntry;

import java.util.Calendar;
import java.util.Date;
import java.util.Map;
import java.util.logging.Logger;

/**
 * User: domenicosolazzo
 */
public class VolumeCountingBolt extends BaseRichBolt {

    private static final long serialVersionUID = 1L;
    public static Logger LOG = Logger.getLogger(VolumeCountingBolt.class.toString());
    private OutputCollector collector;

    public static final String FIELD_ROW_KEY = "RowKey";
    public static final String FIELD_COLUMN = "Column";
    public static final String FIELD_INCREMENT = "IncrementAmount";


    @Override
    public void prepare(Map map, TopologyContext topologyContext, OutputCollector outputCollector) {
        this.collector = outputCollector;
    }

    @Override
    public void execute(Tuple tuple) {
        LogEntry entry = (LogEntry) tuple.getValueByField(FieldNames.LOG_ENTRY);
        collector.emit(new Values(getMinuteForTime(entry.getTimestamp()), entry.getSource(),VolumeCountingBolt.serialVersionUID));
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(FIELD_ROW_KEY, FIELD_COLUMN, FIELD_INCREMENT));
    }

    public static Long getMinuteForTime(Date time) {
        Calendar c = Calendar.getInstance();
        c.setTime(time);
        c.set(Calendar.SECOND,0);
        c.set(Calendar.MILLISECOND, 0);
        return c.getTimeInMillis();
    }
}
