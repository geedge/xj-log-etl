package cn.ac.iie.storm.bolt.index;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.Gauge;
import io.prometheus.client.exporter.PushGateway;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class GkCountBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkCountBolt.class);

    private long gkCount= 0L;
    private long gkSendCount = 0L;
    private long gkSendFail = 0L;
    private long gkParseFail = 0L;

    private long gk_http_success=0L;
    private long gk_http_fail=0L;
    private long gk_ip_success=0L;
    private long gk_ip_fail=0L;
    private long gk_dns_success=0L;
    private long gk_dns_fail=0L;
    private long gk_mail_success=0L;
    private long gk_mail_fail=0L;
    private long gk_vpn_success=0L;
    private long gk_vpn_fail=0L;
    private long gk_jstx_success=0L;
    private long gk_jstx_fail=0L;
    private long gk_ftp_success=0L;
    private long gk_ftp_fail=0L;
    private long gk_wygjc_success=0L;
    private long gk_wygjc_fail=0L;
    private long gk_ssl_success=0L;
    private long gk_ssl_fail=0L;



    static final Gauge GKInputLog = Gauge.build()
            .name("gkInputLog").labelNames("ServName","Duration").help("Control the intput log volume, the unit is slip").register();

    static final Gauge GKOutputLog = Gauge.build()
            .name("gkOutputLog").labelNames("ServName","Duration").help("Control the output log volume, the unit is slip").register();

    static final Gauge GKHttpSuccess = Gauge.build()
            .name("gkHttpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKHttpFail = Gauge.build()
            .name("gkHttpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKIpSuccess = Gauge.build()
            .name("gkIpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKIpFail = Gauge.build()
            .name("gkIpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKSslSuccess = Gauge.build()
            .name("gkSslSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKSslFail = Gauge.build()
            .name("gkSslFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKVpnSuccess = Gauge.build()
            .name("gkVpnSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKVpnFail = Gauge.build()
            .name("gkVpnFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKDnsSuccess = Gauge.build()
            .name("gkDnsSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKDnsFail = Gauge.build()
            .name("gkDnsFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKMailSuccess = Gauge.build()
            .name("gkMailSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKMailFail = Gauge.build()
            .name("gkMailFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKJstxSuccess = Gauge.build()
            .name("gkJstxSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKJstxFail = Gauge.build()
            .name("gkJstxFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKFtpSuccess = Gauge.build()
            .name("gkFtpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKFtpFail = Gauge.build()
            .name("gkFtpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKWygjcSuccess = Gauge.build()
            .name("gkWygjcSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKWygjcFail = Gauge.build()
            .name("gkWygjcFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge GKSendFail = Gauge.build()
            .name("gkSendFail").labelNames("ServName","Duration").help("Program receiving flow rate, the unit is Mbps").register();

    static final Gauge GKParseFail = Gauge.build()
            .name("gkParseFail").labelNames("ServName","Duration").help("Program output flow rate, the unit is Mbps").register();





    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


        if ((TupleUtils.isTick(tuple) && gkCount != 0L)) {
            gkSendFail = gk_http_fail +  gk_ip_fail + gk_ssl_fail + gk_dns_fail + gk_ftp_fail + gk_jstx_fail + gk_mail_fail + gk_vpn_fail + gk_wygjc_fail;

            gkSendCount = gk_http_success + gk_ip_success + gk_ssl_success + gk_dns_success + gk_ftp_success + gk_jstx_success + gk_mail_success +gk_vpn_success + gk_wygjc_success;


            GKInputLog.labels("gk-input-count", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gkCount);

            GKOutputLog.labels("gk-output-count", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gkSendCount);

            GKSendFail.labels("gk-send-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gkSendFail);
            GKParseFail.labels("gk-parse-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(gkParseFail);


            GKHttpSuccess.labels("gk-http-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_http_success);
            GKHttpFail.labels("gk-http-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_http_fail);

            GKIpSuccess.labels("gk-ip-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ip_success);
            GKIpFail.labels("gk-ip-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ip_fail);

           GKSslSuccess.labels("gk-ssl-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ssl_success);
           GKSslFail.labels("gk-ssl-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ssl_fail);

           GKDnsSuccess.labels("gk-dns-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_dns_success);
           GKDnsFail.labels("gk-dns-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_dns_fail);

           GKVpnSuccess.labels("gk-vpn-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_vpn_success);
           GKVpnFail.labels("gk-vpn-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_vpn_fail);


           GKMailSuccess.labels("gk-mail-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_mail_success);
           GKMailFail.labels("gk-mail-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_mail_fail);

           GKJstxSuccess.labels("gk-jstx-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_jstx_success);
           GKJstxFail.labels("gk-jstx-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_jstx_fail);

           GKFtpSuccess.labels("gk-ftp-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ftp_success);
           GKFtpFail.labels("gk-ftp-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_ftp_fail);

           GKWygjcSuccess.labels("gk-wygjc-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_wygjc_success);
           GKWygjcFail.labels("gk-wygjc-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(gk_wygjc_fail);


            try {
                push();

            } catch (IOException e) {
                e.printStackTrace();
            }
            gkCount= 0L;
            gkSendCount = 0L;
            gkSendFail = 0L;
            gkParseFail = 0L;
            gk_http_success=0L;
            gk_http_fail=0L;
            gk_ip_success=0L;
            gk_ip_fail=0L;
            gk_dns_success=0L;
            gk_dns_fail=0L;
            gk_mail_success=0L;
            gk_mail_fail=0L;
            gk_vpn_success=0L;
            gk_vpn_fail=0L;
            gk_jstx_success=0L;
            gk_jstx_fail=0L;
            gk_ftp_success=0L;
            gk_ftp_fail=0L;
            gk_wygjc_success=0L;
            gk_wygjc_fail=0L;
            gk_ssl_success=0L;
            gk_ssl_fail=0L;

        } else {
            String sourceComponent = tuple.getSourceComponent();
            switch (sourceComponent) {
                case "GK-PARSE":
                    if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_COUNT_STREAMID)) {
                        gkCount = gkCount + tuple.getLongByField("count");
                        gkParseFail = gkParseFail + tuple.getLongByField("loadfail");
                    }
                    break;

                case "GK-SEND":
                    gk_dns_success = gk_dns_success + tuple.getLongByField("gk_dns_success");
                    gk_dns_fail = gk_dns_fail + tuple.getLongByField("gk_dns_fail");

                    gk_mail_success = gk_mail_success + tuple.getLongByField("gk_mail_success");
                    gk_mail_fail = gk_mail_fail + tuple.getLongByField("gk_mail_fail");

                    gk_vpn_success = gk_vpn_success + tuple.getLongByField("gk_vpn_success");
                    gk_vpn_fail = gk_vpn_fail + tuple.getLongByField("gk_vpn_fail");

                    gk_jstx_success = gk_jstx_success + tuple.getLongByField("gk_jstx_success");
                    gk_jstx_fail = gk_jstx_fail + tuple.getLongByField("gk_jstx_fail");

                    gk_jstx_success = gk_jstx_success + tuple.getLongByField("gk_jstx_success");
                    gk_jstx_fail = gk_jstx_fail + tuple.getLongByField("gk_jstx_fail");

                    gk_ftp_success = gk_ftp_success + tuple.getLongByField("gk_ftp_success");
                    gk_ftp_fail = gk_ftp_fail + tuple.getLongByField("gk_ftp_fail");

                    gk_wygjc_success = gk_wygjc_success + tuple.getLongByField("gk_wygjc_success");
                    gk_wygjc_fail = gk_wygjc_fail + tuple.getLongByField("gk_wygjc_fail");
                    break;

                case "GK-IP-SEND":
                    gk_ip_success = gk_ip_success + tuple.getLongByField("gk_ip_success");
                    gk_ip_fail = gk_ip_fail + tuple.getLongByField("gk_ip_fail");
                    break;

                case "GK-HTTP-SEND":
                    gk_http_success = gk_http_success + tuple.getLongByField("gk_http_success");
                    gk_http_fail = gk_http_fail + tuple.getLongByField("gk_http_fail");
                    break;

                case "GK-SSL-SEND":
                    gk_ssl_success = gk_ssl_success + tuple.getLongByField("gk_ssl_success");
                    gk_ssl_fail = gk_ssl_fail + tuple.getLongByField("gk_ssl_fail");
                    break;
                default:
                    break;
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
