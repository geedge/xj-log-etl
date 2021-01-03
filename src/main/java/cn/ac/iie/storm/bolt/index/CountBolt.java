package cn.ac.iie.storm.bolt.index;



import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.HTTPServer;
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

public class CountBolt extends BaseBasicBolt {

    private final static Logger logger = Logger.getLogger(CountBolt.class);
    private long floodingcount = 0L;
    private long ybinputsize = 0L;
    private long yboutputsize = 0L;
    private long floodingsucesscount = 0L;
    private long floodingloadfailcount = 0L;
    private long count = 0L;
    private long sucesscount = 0L;
    private long loadfailcount = 0L;
    private long timestamp;
    InetAddress ip=null;
    private String localip;
    private float inputrate;
    private float outputrate;
    static final Gauge InputRate = Gauge.build()
            .name("InputRate").labelNames("ServName","IP","Duration").help("Program receiving flow rate, the unit is Mbps").register();


    static final Gauge OutputRate = Gauge.build()
            .name("OutputRate").labelNames("ServName","IP","Duration").help("Program output flow rate, the unit is Mbps").register();


    static final Gauge YBInputLog = Gauge.build()
            .name("YBInputLog").labelNames("ServName","IP","Duration").help("Generally input log volume size, the unit is slip").register();

    static final Gauge YBOutputLog = Gauge.build()
            .name("YBOutputLog").labelNames("ServName","IP","Duration").help("The general output log volume, the unit is slip").register();

    static final Gauge Timestamp = Gauge.build()
            .name("Timestamp").labelNames("ServName","IP","Duration").help("Unix timestamp (second level)").register();





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

        if((TupleUtils.isTick(input)&&floodingcount!=0L)){


            timestamp=System.currentTimeMillis()/1000;
            inputrate=ybinputsize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS;
            outputrate = yboutputsize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS;

            InputRate.labels("flooding-etl",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(inputrate);
            OutputRate.labels("flooding-etl",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(outputrate);
            YBInputLog.labels("flooding-etl",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(floodingcount);
            YBOutputLog.labels("flooding-etl",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(floodingsucesscount);
            Timestamp.labels("flooding-etl",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).setToCurrentTime();
            System.out.println(inputrate);
            try {
                push();
            } catch (IOException e) {
                e.printStackTrace();
            }
            ybinputsize=0L;
            yboutputsize =0L;
            inputrate =0;
            outputrate =0;
            floodingcount = 0L;
            floodingsucesscount = 0L;
            floodingloadfailcount = 0L;
        }else {
            if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_DNS_COMPONENTID)) {
                Long dnscount = input.getLongByField(StreamAggregateConfig.FLOODING_DNSCOUNT_FIELD);
                Long dnsloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_DNSLOADFAIL_FIELD);
                Long dnsdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_DNSDATASIZE_FIELD);

                yboutputsize = yboutputsize + dnsdatasize;
                floodingcount = floodingcount + dnscount;
                floodingloadfailcount = floodingloadfailcount + dnsloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_FTP_COMPONENTID)) {
                Long ftpcount = input.getLongByField(StreamAggregateConfig.FLOODING_FTPCOUNT_FIELD);
                Long ftploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_FTPLOADFAIL_FIELD);
                Long ftpdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_FTPDATASIZE_FIELD);

                yboutputsize = yboutputsize + ftpdatasize;
                floodingcount = floodingcount + ftpcount;
                floodingloadfailcount = floodingloadfailcount + ftploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            } else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID)) {
                Long httpcount = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPCOUNT_FIELD);
                Long httploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPLOADFAIL_FIELD);
                Long httpdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPDATASIZE_FIELD);

                yboutputsize = yboutputsize + httpdatasize;
                floodingcount = floodingcount + httpcount;
                floodingloadfailcount = floodingloadfailcount + httploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_IP_COMPONENTID)) {
                Long ipcount = input.getLongByField(StreamAggregateConfig.FLOODING_IPCOUNT_FIELD);
                Long iploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_IPLOADFAIL_FIELD);
                Long ipdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_IPDATASIZE_FIELD);

                yboutputsize = yboutputsize +ipdatasize;
                floodingcount = floodingcount + ipcount;
                floodingloadfailcount = floodingloadfailcount + iploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID)) {
                Long jstxcount = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXCOUNT_FIELD);
                Long jstxloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXLOADFAIL_FIELD);
                Long jstxdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXDATASIZE_FIELD);

                yboutputsize = yboutputsize + jstxdatasize;
                floodingcount = floodingcount + jstxcount;
                floodingloadfailcount = floodingloadfailcount + jstxloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID)) {
                Long mailcount = input.getLongByField(StreamAggregateConfig.FLOODING_MAILCOUNT_FIELD);
                Long mailloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_MAILLOADFAIL_FIELD);
                Long maildatasize = input.getLongByField(StreamAggregateConfig.FLOODING_MAILDATASIZE_FIELD);

                yboutputsize = yboutputsize +maildatasize;
                floodingcount = floodingcount + mailcount;
                floodingloadfailcount = floodingloadfailcount + mailloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_SSL_COMPONENTID)) {
                Long sslcount = input.getLongByField(StreamAggregateConfig.FLOODING_SSLCOUNT_FIELD);
                Long sslloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_SSLLOADFAIL_FIELD);
                Long ssldatasize = input.getLongByField(StreamAggregateConfig.FLOODING_SSLDATASIZE_FIELD);

                yboutputsize = yboutputsize + ssldatasize;
                floodingcount = floodingcount + sslcount;
                floodingloadfailcount = floodingloadfailcount + sslloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_VPN_COMPONENTID)) {
                Long vpncount = input.getLongByField(StreamAggregateConfig.FLOODING_VPNCOUNT_FIELD);
                Long vpnloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_VPNLOADFAIL_FIELD);
                Long vpndatasize = input.getLongByField(StreamAggregateConfig.FLOODING_VPNDATASIZE_FIELD);

                yboutputsize = yboutputsize +vpndatasize;
                floodingcount = floodingcount + vpncount;
                floodingloadfailcount = floodingloadfailcount + vpnloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else  if(input.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID)){
                //一般日志总的输入
                ybinputsize =ybinputsize + input.getLongByField(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    public void push() throws IOException {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        PushGateway pg = new PushGateway(StreamAggregateConfig.PROMETHEUS_PUSHGATEWAY_ADDRESS);
        pg.push(registry, "XJ-ETL-STATUS" );

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
