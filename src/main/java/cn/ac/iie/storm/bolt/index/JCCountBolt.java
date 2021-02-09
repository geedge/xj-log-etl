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

public class JCCountBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCCountBolt.class);

    private long jcCount= 0L;
    private long jcSendCount = 0L;
    private long jcloadFail = 0L;



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




    static final Gauge jcInputLog = Gauge.build()
            .name("jcInputLog").labelNames("ServName","Duration").help("Control the intput log volume, the unit is slip").register();

    static final Gauge jcOutputLog = Gauge.build()
            .name("jcOutputLog").labelNames("ServName","Duration").help("Control the output log volume, the unit is slip").register();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


        if((TupleUtils.isTick(tuple)&&jcCount!=0L)){

            jcInputLog.labels("jc-input-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcCount);
            jcOutputLog.labels("jc-output-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcSendCount);
            try {
                push();
            } catch (IOException e) {
                e.printStackTrace();
            }
            jcCount= 0L;
            jcSendCount = 0L;
            jcloadFail = 0L;
        }else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_COUNT_STREAMID)){
                jcCount = jcCount + tuple.getLongByField("count");
                jcloadFail = jcloadFail + tuple.getLongByField("loadfail");
                jcSendCount = jcCount - jcloadFail;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    public void push() throws IOException {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        PushGateway pg = new PushGateway(StreamAggregateConfig.PROMETHEUS_PUSHGATEWAY_ADDRESS);
        pg.push(registry, "JC-ETL-METRICS" );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
