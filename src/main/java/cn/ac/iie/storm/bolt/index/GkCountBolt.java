package cn.ac.iie.storm.bolt.index;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class GkCountBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkCountBolt.class);

    private long gkCount= 0L;
    private long gkSendCount = 0L;
    private long gkloadFail = 0L;
//    @Override
//    public void prepare(Map stormConf, TopologyContext context) {
//        try {
//            ip=ip.getLocalHost();
//        } catch (UnknownHostException e) {
//            e.printStackTrace();
//        }
//        String localname=ip.getHostName();
//        localip=ip.getHostAddress();
//    }


    static final Gauge GKInputLog = Gauge.build()
            .name("gkInputLog").labelNames("ServName","Duration").help("Control the intput log volume, the unit is slip").register();

    static final Gauge GKOutputLog = Gauge.build()
            .name("gkOutputLog").labelNames("ServName","Duration").help("Control the output log volume, the unit is slip").register();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


        if((TupleUtils.isTick(tuple)&&gkCount!=0L)){
            GKInputLog.labels("gk-input-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gkCount);
            GKOutputLog.labels("gk-output-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gkSendCount);
            try {
                push();

            } catch (IOException e) {
                e.printStackTrace();
            }
            gkCount= 0L;
            gkSendCount = 0L;
            gkloadFail = 0L;

        }else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_COUNT_STREAMID)){
                gkCount = gkCount + tuple.getLongByField("count");
                gkloadFail = gkloadFail + tuple.getLongByField("loadfail");
                gkSendCount = gkCount - gkloadFail;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void push() throws IOException {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        PushGateway pg = new PushGateway(StreamAggregateConfig.PROMETHEUS_PUSHGATEWAY_ADDRESS);
        pg.push(registry, "GK-ETL-METRICS" );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
