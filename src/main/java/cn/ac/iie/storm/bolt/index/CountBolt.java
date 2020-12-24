package cn.ac.iie.storm.bolt.index;

import cn.ac.iie.storm.bolt.flooding.ParseFloodingBolt;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.MetricsServlet;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {

    private final static Logger logger = Logger.getLogger(CountBolt.class);
    private Long floodingcount = 0L;
    private Long floodingsucesscount = 0L;
    private Long floodingloadfailcount = 0L;
    private Long count = 0L;
    private Long sucesscount = 0L;
    private Long loadfailcount = 0L;
    InetAddress ip=null;
    private String localip;

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

            System.out.println("全部数据的总数"+floodingcount);
            System.out.println("全部数据的成功数"+floodingsucesscount);
            System.out.println("全部数据的失败数"+floodingloadfailcount);
            System.out.println("本机的ip地址为"+localip);
            floodingcount = 0L;
            floodingsucesscount = 0L;
            floodingloadfailcount = 0L;
        }else {
            if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_DNS_COMPONENTID)) {
                Long dnscount = input.getLongByField(StreamAggregateConfig.FLOODING_DNSCOUNT_FIELD);
                Long dnsloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_DNSLOADFAIL_FIELD);
                floodingcount = floodingcount + dnscount;
                floodingloadfailcount = floodingloadfailcount + dnsloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_FTP_COMPONENTID)) {
                Long ftpcount = input.getLongByField(StreamAggregateConfig.FLOODING_FTPCOUNT_FIELD);
                Long ftploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_FTPLOADFAIL_FIELD);
                floodingcount = floodingcount + ftpcount;
                floodingloadfailcount = floodingloadfailcount + ftploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            } else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID)) {
                Long httpcount = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPCOUNT_FIELD);
                Long httploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPLOADFAIL_FIELD);
                floodingcount = floodingcount + httpcount;
                floodingloadfailcount = floodingloadfailcount + httploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_IP_COMPONENTID)) {
                Long ipcount = input.getLongByField(StreamAggregateConfig.FLOODING_IPCOUNT_FIELD);
                Long iploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_IPLOADFAIL_FIELD);
                floodingcount = floodingcount + ipcount;
                floodingloadfailcount = floodingloadfailcount + iploadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID)) {
                Long jstxcount = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXCOUNT_FIELD);
                Long jstxloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXLOADFAIL_FIELD);
                floodingcount = floodingcount + jstxcount;
                floodingloadfailcount = floodingloadfailcount + jstxloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID)) {
                Long mailcount = input.getLongByField(StreamAggregateConfig.FLOODING_MAILCOUNT_FIELD);
                Long mailloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_MAILLOADFAIL_FIELD);
                floodingcount = floodingcount + mailcount;
                floodingloadfailcount = floodingloadfailcount + mailloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;

            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_SSL_COMPONENTID)) {
                Long sslcount = input.getLongByField(StreamAggregateConfig.FLOODING_SSLCOUNT_FIELD);
                Long sslloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_SSLLOADFAIL_FIELD);
                floodingcount = floodingcount + sslcount;
                floodingloadfailcount = floodingloadfailcount + sslloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;

            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_VPN_COMPONENTID)) {
                Long vpncount = input.getLongByField(StreamAggregateConfig.FLOODING_VPNCOUNT_FIELD);
                Long vpnloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_VPNLOADFAIL_FIELD);
                floodingcount = floodingcount + vpncount;
                floodingloadfailcount = floodingloadfailcount + vpnloadfail;
                floodingsucesscount = floodingcount - floodingloadfailcount;
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 100);
        return conf;
    }


    private volatile boolean initialized;
    public synchronized void start(int port,long count) {
        if (initialized) {
            return;
        }
        try {
            // expose metrics with http service
            Server server = new Server(port);
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            Gauge.build().name("inprogress_requests").help("Inprogress requests.").register().set(count);
            server.setHandler(context);
            context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
            server.start();
            logger.info(getTime(new Date())+"连接了端口"+port);
            initialized = true;
        } catch (Exception e) {
            logger.error("Failed to create metric http service", e);
        }
    }





        private static String getTime(Date now) {
            SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
            return simpleDateFormat.format(now);
        }
}
