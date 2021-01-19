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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class ConnCountBolt extends BaseBasicBolt {

    private final static Logger logger = Logger.getLogger(ConnCountBolt.class);
    private long connCount= 0L;
    private long ConnSendCount = 0L;
    private long connInputSize = 0L;
    private long connOutputSize = 0L;
    private long connloadFail = 0L;

    InetAddress ip=null;
    private String localip;
    private float inputrate;
    private float outputrate;
    static final Gauge InputRate = Gauge.build()
            .name("InputRate").labelNames("ServName","IP","Duration").help("Program receiving flow rate, the unit is Mbps").register();

    static final Gauge OutputRate = Gauge.build()
            .name("OutputRate").labelNames("ServName","IP","Duration").help("Program output flow rate, the unit is Mbps").register();

    static final Gauge YBInputLog = Gauge.build()
            .name("YBInputLog").labelNames("ServName","IP","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge YBOutputLog = Gauge.build()
            .name("YBOutputLog").labelNames("ServName","IP","Duration").help("The general output log volume, the unit is slip").register();


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            ip=ip.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }
        String localname=ip.getHostName();
             localip=ip.getHostAddress();
    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        if((TupleUtils.isTick(input)&&connCount!=0L)){


            inputrate=(float)connInputSize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;
            outputrate =(float)connInputSize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;

            //一般
            InputRate.labels("conn-input-rate",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(inputrate);
            OutputRate.labels("conn-output-rate",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(outputrate);
            YBInputLog.labels("conn-input-count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(connCount);
            YBOutputLog.labels("conn-output-count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(ConnSendCount);
            try {
                push();
            } catch (IOException e) {
                e.printStackTrace();
            }
           long connCount= 0L;
           long ConnSendCount = 0L;
           long connInputSize = 0L;
           long connOutputSize = 0L;
           long connloadFail = 0L;
        }else {
            if (input.getSourceStreamId().equals(StreamAggregateConfig.CONN_YBInputSize_STREAMID)){
                connInputSize = connInputSize + input.getLongByField("connInputSize");
                connCount = connCount + input.getLongByField("count");
                connloadFail = connloadFail + input.getLongByField("loadFail");
                connOutputSize = connOutputSize + input.getLongByField("outputDataSize");
                ConnSendCount = connCount - connloadFail;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    public void push() throws IOException {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        PushGateway pg = new PushGateway(StreamAggregateConfig.PROMETHEUS_PUSHGATEWAY_ADDRESS);
        pg.push(registry, "CONN-ETL-ME" );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
