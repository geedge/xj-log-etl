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


    private long inputsize = 0L;
    private long outputsize = 0L;


    //gk

    private long gkoutputsize = 0L;
    private long gkinputcount = 0L;
    private long gksucesscount = 0L;
    private long gkloadfailcount = 0L
            ;

    //jc

    private long jcoutputsize = 0L;
    private long jcinputcount = 0L;
    private long jcsucesscount = 0L;
    private long jcloadfailcount = 0L;

    private long safeinputsize = 0L;

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
            .name("YBInputLog").labelNames("ServName","IP","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge YBOutputLog = Gauge.build()
            .name("YBOutputLog").labelNames("ServName","IP","Duration").help("The general output log volume, the unit is slip").register();

    static final Gauge GKInputLog = Gauge.build()
            .name("GKInputLog").labelNames("ServName","IP","Duration").help("Control the intput log volume, the unit is slip").register();


    static final Gauge GKOutputLog = Gauge.build()
            .name("GKOutputLog").labelNames("ServName","IP","Duration").help("Control the output log volume, the unit is slip").register();

    static final Gauge JCInputLog = Gauge.build()
            .name("JCInputLog").labelNames("ServName","IP","Duration").help("Monitor the size of the input log, the unit is slip").register();


    static final Gauge JCOutputLog = Gauge.build()
            .name("JCOutputLog").labelNames("ServName","IP","Duration").help("Monitor the size of the input log, the unit is slip").register();


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


            jcsucesscount = jcinputcount - jcloadfailcount;
            gksucesscount = gkinputcount - gkloadfailcount;
            floodingsucesscount = floodingcount - floodingloadfailcount;

            inputsize = ybinputsize + safeinputsize;
            outputsize = yboutputsize + gkoutputsize + jcoutputsize;


            timestamp=System.currentTimeMillis()/1000;
            inputrate=inputsize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;
            outputrate = outputsize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;


            //一般
            InputRate.labels("Total input rate",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(inputrate);
            OutputRate.labels("Total output rate",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(outputrate);
            YBInputLog.labels("flooding input count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(floodingcount);
            YBOutputLog.labels("flooding output count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(floodingsucesscount);
            Timestamp.labels("timestamp",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).setToCurrentTime();

            GKInputLog.labels("GK input count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gkinputcount);
            GKOutputLog.labels("GK output count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gksucesscount);

            JCInputLog.labels("JC input count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcinputcount);
            JCOutputLog.labels("JC output count",localip,String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcsucesscount);

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
                long dnscount = input.getLongByField(StreamAggregateConfig.FLOODING_DNSCOUNT_FIELD);
                long dnsloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_DNSLOADFAIL_FIELD);
                long dnsdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_DNSDATASIZE_FIELD);

                yboutputsize = yboutputsize + dnsdatasize;
                floodingcount = floodingcount + dnscount;
                floodingloadfailcount = floodingloadfailcount + dnsloadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_FTP_COMPONENTID)) {
                long ftpcount = input.getLongByField(StreamAggregateConfig.FLOODING_FTPCOUNT_FIELD);
                long ftploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_FTPLOADFAIL_FIELD);
                long ftpdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_FTPDATASIZE_FIELD);

                yboutputsize = yboutputsize + ftpdatasize;
                floodingcount = floodingcount + ftpcount;
                floodingloadfailcount = floodingloadfailcount + ftploadfail;
            } else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID)) {
                long httpcount = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPCOUNT_FIELD);
                long httploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPLOADFAIL_FIELD);
                long httpdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_HTTPDATASIZE_FIELD);

                yboutputsize = yboutputsize + httpdatasize;
                floodingcount = floodingcount + httpcount;
                floodingloadfailcount = floodingloadfailcount + httploadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_IP_COMPONENTID)) {
                long ipcount = input.getLongByField(StreamAggregateConfig.FLOODING_IPCOUNT_FIELD);
                long iploadfail = input.getLongByField(StreamAggregateConfig.FLOODING_IPLOADFAIL_FIELD);
                long ipdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_IPDATASIZE_FIELD);

                yboutputsize = yboutputsize +ipdatasize;
                floodingcount = floodingcount + ipcount;
                floodingloadfailcount = floodingloadfailcount + iploadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID)) {
                long jstxcount = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXCOUNT_FIELD);
                long jstxloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXLOADFAIL_FIELD);
                long jstxdatasize = input.getLongByField(StreamAggregateConfig.FLOODING_JSTXDATASIZE_FIELD);

                yboutputsize = yboutputsize + jstxdatasize;
                floodingcount = floodingcount + jstxcount;
                floodingloadfailcount = floodingloadfailcount + jstxloadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID)) {
                long mailcount = input.getLongByField(StreamAggregateConfig.FLOODING_MAILCOUNT_FIELD);
                long mailloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_MAILLOADFAIL_FIELD);
                long maildatasize = input.getLongByField(StreamAggregateConfig.FLOODING_MAILDATASIZE_FIELD);

                yboutputsize = yboutputsize +maildatasize;
                floodingcount = floodingcount + mailcount;
                floodingloadfailcount = floodingloadfailcount + mailloadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_SSL_COMPONENTID)) {
                long sslcount = input.getLongByField(StreamAggregateConfig.FLOODING_SSLCOUNT_FIELD);
                long sslloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_SSLLOADFAIL_FIELD);
                long ssldatasize = input.getLongByField(StreamAggregateConfig.FLOODING_SSLDATASIZE_FIELD);

                yboutputsize = yboutputsize + ssldatasize;
                floodingcount = floodingcount + sslcount;
                floodingloadfailcount = floodingloadfailcount + sslloadfail;
            }else if (input.getSourceComponent().equals(StreamAggregateConfig.FLOODING_VPN_COMPONENTID)) {
                long vpncount = input.getLongByField(StreamAggregateConfig.FLOODING_VPNCOUNT_FIELD);
                long vpnloadfail = input.getLongByField(StreamAggregateConfig.FLOODING_VPNLOADFAIL_FIELD);
                long vpndatasize = input.getLongByField(StreamAggregateConfig.FLOODING_VPNDATASIZE_FIELD);

                yboutputsize = yboutputsize +vpndatasize;
                floodingcount = floodingcount + vpncount;
                floodingloadfailcount = floodingloadfailcount + vpnloadfail;
            }else  if(input.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID)){
                //一般日志总的输入
                ybinputsize =ybinputsize + input.getLongByField(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID);

            }//gk
//            else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_DNS_COMPONENTID)) {
//                long gkdnscount = input.getLongByField(StreamAggregateConfig.GK_DNSCOUNT_FIELD);
//                long gkdnsloadfail = input.getLongByField(StreamAggregateConfig.GK_DNSLOADFAIL_FIELD);
//                long gkdnsdatasize = input.getLongByField(StreamAggregateConfig.GK_DNSDATASIZE_FIELD);
//
//                gkoutputsize = gkoutputsize +gkdnsdatasize;
//                gkinputcount = gkinputcount + gkdnscount;
//                gkloadfailcount = gkloadfailcount + gkdnsloadfail;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_FTP_COMPONENTID)) {
//                long gkftpcount = input.getLongByField(StreamAggregateConfig.GK_FTPCOUNT_FIELD);
//                long gkftploadfail = input.getLongByField(StreamAggregateConfig.GK_FTPLOADFAIL_FIELD);
//                long gkftpdatasize = input.getLongByField(StreamAggregateConfig.GK_FTPDATASIZE_FIELD);
//
//                gkoutputsize = gkoutputsize +gkftpdatasize;
//                gkinputcount = gkinputcount + gkftpcount;
//                gkloadfailcount = gkloadfailcount + gkftploadfail;
//            } else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_HTTP_COMPONENTID)) {
//                long gkhttpcount = input.getLongByField(StreamAggregateConfig.GK_HTTPCOUNT_FIELD);
//                long gkhttploadfail = input.getLongByField(StreamAggregateConfig.GK_HTTPLOADFAIL_FIELD);
//                long gkhttpdatasize = input.getLongByField(StreamAggregateConfig.GK_HTTPDATASIZE_FIELD);
//
//                gkoutputsize = gkoutputsize +gkhttpdatasize;
//                gkinputcount = gkinputcount + gkhttpcount;
//                gkloadfailcount = gkloadfailcount + gkhttploadfail;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_IP_COMPONENTID)) {
//                long gkipcount = input.getLongByField(StreamAggregateConfig.GK_IPCOUNT_FIELD);
//                long gkiploadfail = input.getLongByField(StreamAggregateConfig.GK_IPLOADFAIL_FIELD);
//                long gkipdatasize = input.getLongByField(StreamAggregateConfig.GK_IPDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkipcount;
//                gkloadfailcount = gkloadfailcount + gkiploadfail;
//                gkoutputsize = gkoutputsize +gkipdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_JSTX_COMPONENTID)) {
//                long gkjstxcount = input.getLongByField(StreamAggregateConfig.GK_JSTXCOUNT_FIELD);
//                long gkjstxloadfail = input.getLongByField(StreamAggregateConfig.GK_JSTXLOADFAIL_FIELD);
//                long gkjstxdatasize = input.getLongByField(StreamAggregateConfig.GK_JSTXDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkjstxcount;
//                gkloadfailcount = gkloadfailcount + gkjstxloadfail;
//                gkoutputsize = gkoutputsize +gkjstxdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_MAIL_COMPONENTID)) {
//                long gkmailcount = input.getLongByField(StreamAggregateConfig.GK_MAILCOUNT_FIELD);
//                long gkmailloadfail = input.getLongByField(StreamAggregateConfig.GK_MAILLOADFAIL_FIELD);
//                long gkMAILdatasize = input.getLongByField(StreamAggregateConfig.GK_MAILDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkmailcount;
//                gkloadfailcount = gkloadfailcount + gkmailloadfail;
//                gkoutputsize = gkoutputsize +gkMAILdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_SSL_COMPONENTID)) {
//                long gksslcount = input.getLongByField(StreamAggregateConfig.GK_SSLCOUNT_FIELD);
//                long gkSSLloadfail = input.getLongByField(StreamAggregateConfig.GK_SSLLOADFAIL_FIELD);
//                long gkSSLdatasize = input.getLongByField(StreamAggregateConfig.GK_SSLDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gksslcount;
//                gkloadfailcount = gkloadfailcount + gkSSLloadfail;
//                gkoutputsize = gkoutputsize +gkSSLdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_VPN_COMPONENTID)) {
//                long gkvpncount = input.getLongByField(StreamAggregateConfig.GK_VPNCOUNT_FIELD);
//                long gkvpnloadfail = input.getLongByField(StreamAggregateConfig.GK_VPNLOADFAIL_FIELD);
//                long gkvpndatasize = input.getLongByField(StreamAggregateConfig.GK_VPNDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkvpncount;
//                gkloadfailcount = gkloadfailcount + gkvpnloadfail;
//                gkoutputsize = gkoutputsize +gkvpndatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_VPN_COMPONENTID)) {
//                long gkvpncount = input.getLongByField(StreamAggregateConfig.GK_VPNCOUNT_FIELD);
//                long gkvpnloadfail = input.getLongByField(StreamAggregateConfig.GK_VPNLOADFAIL_FIELD);
//                long gkvpndatasize = input.getLongByField(StreamAggregateConfig.GK_VPNDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkvpncount;
//                gkloadfailcount = gkloadfailcount + gkvpnloadfail;
//                gkoutputsize = gkoutputsize +gkvpndatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.GK_WYGJC_COMPONENTID)) {
//                long gkwygjccount = input.getLongByField(StreamAggregateConfig.GK_WYGJCCOUNT_FIELD);
//                long gkwygjcloadfail = input.getLongByField(StreamAggregateConfig.GK_WYGJCLOADFAIL_FIELD);
//                long gkwygjcdatasize = input.getLongByField(StreamAggregateConfig.GK_WYGJCDATASIZE_FIELD);
//
//                gkinputcount = gkinputcount + gkwygjccount;
//                gkloadfailcount = gkloadfailcount + gkwygjcloadfail;
//                gkoutputsize = gkoutputsize +gkwygjcdatasize;
//            }
//            //jc
//            else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_DNS_COMPONENTID)) {
//                long jcdnscount = input.getLongByField(StreamAggregateConfig.JC_DNSCOUNT_FIELD);
//                long jcdnsloadfail = input.getLongByField(StreamAggregateConfig.JC_DNSLOADFAIL_FIELD);
//                long jcdnsdatasize = input.getLongByField(StreamAggregateConfig.JC_DNSDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcdnscount;
//                jcloadfailcount = jcloadfailcount + jcdnsloadfail;
//                jcoutputsize = jcoutputsize +jcdnsdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_FTP_COMPONENTID)) {
//                long jcftpcount = input.getLongByField(StreamAggregateConfig.JC_FTPCOUNT_FIELD);
//                long jcftploadfail = input.getLongByField(StreamAggregateConfig.JC_FTPLOADFAIL_FIELD);
//                long jcftpdatasize = input.getLongByField(StreamAggregateConfig.JC_FTPDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcftpcount;
//                jcloadfailcount = jcloadfailcount + jcftploadfail;
//                jcoutputsize = jcoutputsize +jcftpdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_HTTP_COMPONENTID)) {
//                long jchttpcount = input.getLongByField(StreamAggregateConfig.JC_HTTPCOUNT_FIELD);
//                long jchttploadfail = input.getLongByField(StreamAggregateConfig.JC_HTTPLOADFAIL_FIELD);
//                long jchttpdatasize = input.getLongByField(StreamAggregateConfig.JC_HTTPDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jchttpcount;
//                jcloadfailcount = jcloadfailcount + jchttploadfail;
//                jcoutputsize = jcoutputsize +jchttpdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_IP_COMPONENTID)) {
//                long jcipcount = input.getLongByField(StreamAggregateConfig.JC_IPCOUNT_FIELD);
//                long jciploadfail = input.getLongByField(StreamAggregateConfig.JC_IPLOADFAIL_FIELD);
//                long jcipdatasize = input.getLongByField(StreamAggregateConfig.JC_IPDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcipcount;
//                jcloadfailcount = jcloadfailcount + jciploadfail;
//                jcoutputsize = jcoutputsize +jcipdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_JSTX_COMPONENTID)) {
//                long jcjstxcount = input.getLongByField(StreamAggregateConfig.JC_JSTXCOUNT_FIELD);
//                long jcjstxloadfail = input.getLongByField(StreamAggregateConfig.JC_JSTXLOADFAIL_FIELD);
//                long jcjstxdatasize = input.getLongByField(StreamAggregateConfig.JC_JSTXDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcjstxcount;
//                jcloadfailcount = jcloadfailcount + jcjstxloadfail;
//                jcoutputsize = jcoutputsize +jcjstxdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_MAIL_COMPONENTID)) {
//                long jcmailcount = input.getLongByField(StreamAggregateConfig.JC_MAILCOUNT_FIELD);
//                long jcmailloadfail = input.getLongByField(StreamAggregateConfig.JC_MAILLOADFAIL_FIELD);
//                long jcmaildatasize = input.getLongByField(StreamAggregateConfig.JC_MAILDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcmailcount;
//                jcloadfailcount = jcloadfailcount + jcmailloadfail;
//                jcoutputsize = jcoutputsize +jcmaildatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_MEDIA_COMPONENTID)) {
//                long jcmediacount = input.getLongByField(StreamAggregateConfig.JC_MEDIACOUNT_FIELD);
//                long jcmedialoadfail = input.getLongByField(StreamAggregateConfig.JC_MEDIALOADFAIL_FIELD);
//                long jcmediadatasize = input.getLongByField(StreamAggregateConfig.JC_MEDIADATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcmediacount;
//                jcloadfailcount = jcloadfailcount + jcmedialoadfail;
//                jcoutputsize = jcoutputsize +jcmediadatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_PIC_COMPONENTID)) {
//                long jcpiccount = input.getLongByField(StreamAggregateConfig.JC_PICCOUNT_FIELD);
//                long jcpicloadfail = input.getLongByField(StreamAggregateConfig.JC_PICLOADFAIL_FIELD);
//                long jcpicdatasize = input.getLongByField(StreamAggregateConfig.JC_PICDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcpiccount;
//                jcloadfailcount = jcloadfailcount + jcpicloadfail;
//                jcoutputsize = jcoutputsize +jcpicdatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_SSL_COMPONENTID)) {
//                long jcsslcount = input.getLongByField(StreamAggregateConfig.JC_SSLCOUNT_FIELD);
//                long jcsslloadfail = input.getLongByField(StreamAggregateConfig.JC_SSLLOADFAIL_FIELD);
//                long jcssldatasize = input.getLongByField(StreamAggregateConfig.JC_SSLDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcsslcount;
//                jcloadfailcount = jcloadfailcount + jcsslloadfail;
//                jcoutputsize = jcoutputsize +jcssldatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_VPN_COMPONENTID)) {
//                long jcvpncount = input.getLongByField(StreamAggregateConfig.JC_VPNCOUNT_FIELD);
//                long jcvpnloadfail = input.getLongByField(StreamAggregateConfig.JC_VPNLOADFAIL_FIELD);
//                long jcvpndatasize = input.getLongByField(StreamAggregateConfig.JC_VPNDATASIZE_FIELD);
//
//                jcinputcount = jcinputcount + jcvpncount;
//                jcloadfailcount = jcloadfailcount + jcvpnloadfail;
//                jcoutputsize = jcoutputsize +jcvpndatasize;
//            }else if (input.getSourceComponent().equals(StreamAggregateConfig.JC_WYGJC_COMPONENTID)) {
//                long jcwygjccount = input.getLongByField(StreamAggregateConfig.JC_WYGJCCOUNT_FIELD);
//                long jcwygjcloadfail = input.getLongByField(StreamAggregateConfig.JC_WYGJCLOADFAIL_FIELD);
//                long jcwygjcdatasize = input.getLongByField(StreamAggregateConfig.JC_WYGJCDATASIZE_FIELD);
//                jcinputcount = jcinputcount + jcwygjccount;
//                jcloadfailcount = jcloadfailcount + jcwygjcloadfail;
//                jcoutputsize = jcoutputsize +jcwygjcdatasize;
//            }
//            else  if(input.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID)){
//                //一般日志总的输入
//                safeinputsize =safeinputsize + input.getLongByField(StreamAggregateConfig.SAFE_INPUTSIZE_STREAMID);
//
//            }
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
