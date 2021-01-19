package cn.ac.iie.storm.bolt.index;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import io.prometheus.client.Gauge;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class GkCountBolt extends BaseBasicBolt {

    private final static Logger logger = Logger.getLogger(GkCountBolt.class);
    static final Gauge InputRate = Gauge.build()
            .name("InputRate").labelNames("ServName","IP","Duration").help("Program receiving flow rate, the unit is Mbps").register();

    static final Gauge OutputRate = Gauge.build()
            .name("OutputRate").labelNames("ServName","IP","Duration").help("Program output flow rate, the unit is Mbps").register();


    static final Gauge GKInputLog = Gauge.build()
            .name("GKInputLog").labelNames("ServName","IP","Duration").help("Control the intput log volume, the unit is slip").register();

    static final Gauge GKOutputLog = Gauge.build()
            .name("GKOutputLog").labelNames("ServName","IP","Duration").help("Control the output log volume, the unit is slip").register();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        GKInputLog.labels("GK-input-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60));
        GKOutputLog.labels("GK-output-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
