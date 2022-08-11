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

public class ConnCountBolt extends BaseBasicBolt {

    private final static Logger logger = Logger.getLogger(ConnCountBolt.class);
    private long connCount= 0L;
    private long ConnSendCount = 0L;
    private long InputSize = 0L;
    private long OutputSize = 0L;
    private long connParseFail = 0L;
    private long connSendFail = 0L;
    private long conn_http_success=0L;
    private long conn_http_fail=0L;
    private long conn_ip_success = 0L;
    private long conn_ip_fail = 0L;
    private long conn_dns_success=0L;
    private long conn_dns_fail=0L;
    private long conn_mail_success=0L;
    private long conn_mail_fail=0L;
    private long conn_vpn_success=0L;
    private long conn_vpn_fail=0L;
    private long conn_jstx_success=0L;
    private long conn_jstx_fail=0L;
    private long conn_ftp_success=0L;
    private long conn_ftp_fail=0L;
    private long conn_ssl_success = 0L;
    private long conn_ssl_fail = 0L;

    private float inputrate;
    private float outputrate;


    private long  yys_error=0L;

    private long ydy_inside=0L;
    private long mddy_inside=0L;

    private long ydy_outside=0L;
    private long mddy_outside=0L;

    private long ydy_unknown=0L;
    private long mddy_unknown=0L;

    static final Gauge InputRate = Gauge.build()
            .name("InputRate").labelNames("ServName","Duration").help("Program receiving flow rate, the unit is Mbps").register();

    static final Gauge OutputRate = Gauge.build()
            .name("OutputRate").labelNames("ServName","Duration").help("Program output flow rate, the unit is Mbps").register();

    static final Gauge connInputLog = Gauge.build()
            .name("connInputLog").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connOutputLog = Gauge.build()
            .name("connOutputLog").labelNames("ServName","Duration").help("The general output log volume, the unit is slip").register();

    static final Gauge connHttpSuccess = Gauge.build()
            .name("connHttpsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connHttpFail = Gauge.build()
            .name("connHttpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connIpSuccess = Gauge.build()
            .name("connIpsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connIpFail = Gauge.build()
            .name("connIpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connSslSuccess = Gauge.build()
            .name("connSslsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connSslFail = Gauge.build()
            .name("connSslFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connVpnSuccess = Gauge.build()
            .name("connVpnsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connVpnFail = Gauge.build()
            .name("connVpnFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connDnsSuccess = Gauge.build()
            .name("connDnssuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connDnsFail = Gauge.build()
            .name("connDnsFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connMailSuccess = Gauge.build()
            .name("connMailsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connMailFail = Gauge.build()
            .name("connMailFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connJstxSuccess = Gauge.build()
            .name("connJstxsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connJstxFail = Gauge.build()
            .name("connJstxFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connFtpSuccess = Gauge.build()
            .name("connFtpsuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge connFtpFail = Gauge.build()
            .name("connFtpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();



    static final Gauge ConnSendFail = Gauge.build()
            .name("ConnSendFail").labelNames("ServName","Duration").help("Program receiving flow rate, the unit is Mbps").register();

    static final Gauge ConnParseFail = Gauge.build()
            .name("connParseFail").labelNames("ServName","Duration").help("Program output flow rate, the unit is Mbps").register();


    static final Gauge ConnYysError = Gauge.build()
            .name("ConnYysError").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();


    static final Gauge ConnYdyInside = Gauge.build()
            .name("ConnYdyInside").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge ConnYdyOutside = Gauge.build()
            .name("ConnYdyOutside").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge ConnYdyUnknown = Gauge.build()
            .name("ConnYdyUnknown").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge ConnMddyInside = Gauge.build()
            .name("ConnMddyInside").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge ConnMddyOutside = Gauge.build()
            .name("ConnmddyOutside").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge ConnMddyUnknown = Gauge.build()
            .name("ConnMddyUnknown").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();



    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {

        if(TupleUtils.isTick(input)){
            inputrate=(float)InputSize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;
            outputrate =(float)OutputSize/StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/125000;
            connSendFail = conn_http_fail +  conn_ip_fail + conn_ssl_fail + conn_dns_fail + conn_ftp_fail + conn_jstx_fail + conn_mail_fail +conn_vpn_fail;
            ConnSendCount = conn_http_success +  conn_ip_success + conn_ssl_success + conn_dns_success + conn_ftp_success + conn_jstx_success + conn_mail_success +conn_vpn_success;
            //一般
            InputRate.labels("input-rate",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(inputrate);
            OutputRate.labels("output-rate",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(outputrate);

            connInputLog.labels("conn-input-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(connCount);
            connOutputLog.labels("conn-output-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(ConnSendCount);

            ConnSendFail.labels("conn-send-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(connSendFail);
            ConnParseFail.labels("conn-parse-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(connParseFail);

            connHttpSuccess.labels("conn-http-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_http_success);
            connHttpFail.labels("conn-http-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_http_fail);

            connIpSuccess.labels("conn-ip-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ip_success);
            connIpFail.labels("conn-ip-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ip_fail);

            connSslSuccess.labels("conn-ssl-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ssl_success);
            connSslFail.labels("conn-ssl-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ssl_fail);

            connVpnSuccess.labels("conn-vpn-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_vpn_success);
            connVpnFail.labels("conn-vpn-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_vpn_fail);

            connDnsSuccess.labels("conn-dns-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_dns_success);
            connDnsFail.labels("conn-dns-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_dns_fail);


            connMailSuccess.labels("conn-mail-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_mail_success);
            connMailFail.labels("conn-mail-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_mail_fail);

            connJstxSuccess.labels("conn-jstx-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_jstx_success);
            connJstxFail.labels("conn-jstx-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_jstx_fail);

            connFtpSuccess.labels("conn-ftp-success",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ftp_success);
            connFtpFail.labels("conn-ftp-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(conn_ftp_fail);

            ConnYysError.labels("conn-yys-error",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(yys_error);

            ConnYdyInside.labels("conn-ydy-inside",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(ydy_inside);
            ConnMddyInside.labels("conn-mddy-inside",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(mddy_inside);

            ConnYdyOutside.labels("conn-ydy-outside",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(ydy_outside);
            ConnMddyOutside.labels("conn-mddy-outside",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(mddy_outside);

            ConnYdyUnknown.labels("conn-ydy-unknow",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(ydy_unknown);
            ConnMddyUnknown.labels("conn-mddy-unknow",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(mddy_unknown);


            try {
                push();
            } catch (IOException e) {
                e.printStackTrace();
            }

            inputrate = 0L;
            outputrate = 0L;
            connCount= 0L;
            ConnSendCount = 0L;
            InputSize = 0L;
            OutputSize = 0L;
            connParseFail = 0L;
            connSendFail = 0L;
            conn_http_success=0L;
            conn_http_fail=0L;
            conn_ip_success = 0L;
            conn_ip_fail = 0L;
            conn_dns_success=0L;
            conn_dns_fail=0L;
            conn_mail_success=0L;
            conn_mail_fail=0L;
            conn_vpn_success=0L;
            conn_vpn_fail=0L;
            conn_jstx_success=0L;
            conn_jstx_fail=0L;
            conn_ftp_success=0L;
            conn_ftp_fail=0L;
            conn_ssl_success = 0L;
            conn_ssl_fail = 0L;

            yys_error=0L;
            ydy_inside=0L;
            mddy_inside=0L;
            ydy_outside=0L;
            mddy_outside=0L;
            ydy_unknown=0L;
            mddy_unknown=0L;
        }else {
            String sourceComponent = input.getSourceComponent();
            switch (sourceComponent){

                case "CONN-PARSE-COMPONENTID":
                    if (input.getSourceStreamId().equals(StreamAggregateConfig.CONN_YBInputSize_STREAMID)){
                        InputSize = InputSize + input.getLongByField("connInputSize");
                        connCount = connCount + input.getLongByField("count");
                        connParseFail = connParseFail + input.getLongByField("loadFail");
                        OutputSize = OutputSize + input.getLongByField("connOutputSize");
                        yys_error = yys_error + input.getLongByField("yys_error");
                        ydy_inside = ydy_inside + input.getLongByField("ydy_inside");
                        ydy_outside = ydy_outside + input.getLongByField("ydy_outside");
                        ydy_unknown = ydy_unknown + input.getLongByField("ydy_unknown");
                        mddy_inside = mddy_inside + input.getLongByField("mddy_inside");
                        mddy_outside = mddy_outside + input.getLongByField("mddy_outside");
                        mddy_unknown = mddy_unknown + input.getLongByField("mddy_unknown");
                    }
                    break;
                case "CONN-SEND":
                    conn_dns_success = conn_dns_success + input.getLongByField("conn_dns_success");
                    conn_dns_fail = conn_dns_fail + input.getLongByField("conn_dns_fail");

                    conn_mail_success = conn_mail_success + input.getLongByField("conn_mail_success");
                    conn_mail_fail = conn_mail_fail + input.getLongByField("conn_mail_fail");

                    conn_vpn_success =conn_vpn_success + input.getLongByField("conn_vpn_success");
                    conn_vpn_fail =conn_vpn_fail + input.getLongByField("conn_vpn_fail");

                    conn_jstx_success =conn_jstx_success + input.getLongByField("conn_jstx_success");
                    conn_jstx_fail =conn_jstx_fail + input.getLongByField("conn_jstx_fail");

                    conn_ftp_success =conn_ftp_success + input.getLongByField("conn_ftp_success");
                    conn_ftp_fail =conn_ftp_fail + input.getLongByField("conn_ftp_fail");
                    break;
                case "CONN-IP-SEND":
                    conn_ip_success = conn_ip_success + input.getLongByField("conn_ip_success");
                    conn_ip_fail = conn_ip_fail + input.getLongByField("conn_ip_fail");
                    break;
                case  "CONN-HTTP-SEND":
                    conn_http_success = conn_http_success + input.getLongByField("conn_http_success");
                    conn_http_fail = conn_http_fail + input.getLongByField("conn_http_fail");
                    break;
                case "CONN-SSL-SEND":
                    conn_ssl_success = conn_ssl_success + input.getLongByField("conn_ssl_success");
                    conn_ssl_fail = conn_ssl_fail + input.getLongByField("conn_ssl_fail");
                    break;
                default:
                    break;
            }

        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    public void push() throws IOException {
        CollectorRegistry registry = CollectorRegistry.defaultRegistry;
        PushGateway pg = new PushGateway(StreamAggregateConfig.PROMETHEUS_PUSHGATEWAY_ADDRESS);
        pg.push(registry, "CONN-ETL-METRICS" );
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
