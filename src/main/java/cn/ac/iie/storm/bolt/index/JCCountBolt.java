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
    private long jcSendFail = 0L;
    private long jcParseFail = 0L;
    private long jc_mail_success=0L;
    private long jc_mail_fail=0L;
    private long jc_vpn_success=0L;
    private long jc_vpn_fail=0L;
    private long jc_jstx_success=0L;
    private long jc_jstx_fail=0L;
    private long jc_ftp_success=0L;
    private long jc_ftp_fail=0L;
    private long jc_wygjc_success=0L;
    private long jc_wygjc_fail=0L;
    private long jc_pic_success=0L;
    private long jc_pic_fail=0L;
    private long jc_media_success=0L;
    private long jc_media_fail=0L;
    private long jc_ip_success=0L;
    private long jc_ip_fail=0L;
    private long jc_dns_success=0L;
    private long jc_dns_fail=0L;
    private long jc_http_success=0L;
    private long jc_http_fail=0L;
    private long jc_ssl_success=0L;
    private long jc_ssl_fail=0L;


    static final Gauge JCInputLog = Gauge.build()
            .name("jcInputLog").labelNames("ServName","Duration").help("Control the intput log volume, the unit is slip").register();

    static final Gauge JCOutputLog = Gauge.build()
            .name("jcOutputLog").labelNames("ServName","Duration").help("Control the output log volume, the unit is slip").register();

    static final Gauge JCHttpSuccess = Gauge.build()
            .name("jcHttpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCHttpFail = Gauge.build()
            .name("JCHttpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCIpSuccess = Gauge.build()
            .name("jcIpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCIpFail = Gauge.build()
            .name("jcIpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCSslSuccess = Gauge.build()
            .name("jcSslSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCSslFail = Gauge.build()
            .name("jcSslFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCVpnSuccess = Gauge.build()
            .name("jcVpnSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCVpnFail = Gauge.build()
            .name("jcVpnFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCDnsSuccess = Gauge.build()
            .name("jcDnsSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCDnsFail = Gauge.build()
            .name("jcDnsFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCMailSuccess = Gauge.build()
            .name("jcMailSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCMailFail = Gauge.build()
            .name("jcMailFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCJstxSuccess = Gauge.build()
            .name("jcJstxSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCJstxFail = Gauge.build()
            .name("jcJstxFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCFtpSuccess = Gauge.build()
            .name("jcFtpSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCFtpFail = Gauge.build()
            .name("jcFtpFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCWygjcSuccess = Gauge.build()
            .name("jcWygjcSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCWygjcFail = Gauge.build()
            .name("jcWygjcFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCSendFail = Gauge.build()
            .name("jcSendFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCParseFail = Gauge.build()
            .name("jcParseFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();


    static final Gauge JCPicSuccess = Gauge.build()
            .name("jcPicSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCPicFail = Gauge.build()
            .name("jcPicFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCMediaSuccess = Gauge.build()
            .name("jcMediaSuccess").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    static final Gauge JCMediaFail = Gauge.build()
            .name("jcMediaFail").labelNames("ServName","Duration").help("The general intput log volume, the unit is slip").register();

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {


        if((TupleUtils.isTick(tuple)&&jcCount!=0L)){

            jcSendFail = jc_http_fail +  jc_ip_fail + jc_ssl_fail + jc_dns_fail + jc_ftp_fail + jc_jstx_fail + jc_mail_fail + jc_vpn_fail + jc_wygjc_fail + jc_media_fail + jc_pic_fail;

            jcSendCount = jc_http_success + jc_ip_success + jc_ssl_success + jc_dns_success + jc_ftp_success + jc_jstx_success + jc_mail_success +jc_vpn_success + jc_wygjc_success + jc_media_success + jc_pic_success;

            JCInputLog.labels("jc-input-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcCount);
            JCOutputLog.labels("jc-output-count",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcSendCount);

            JCSendFail.labels("jc-send-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcSendFail);
            JCParseFail.labels("jc-parse-fail",String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS/60)).set(jcParseFail);


            JCHttpSuccess.labels("jc-http-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_http_success);
            JCHttpFail.labels("jc-http-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_http_fail);

            JCIpSuccess.labels("jc-ip-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ip_success);
            JCIpFail.labels("jc-ip-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ip_fail);

            JCSslSuccess.labels("jc-ssl-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ssl_success);
            JCSslFail.labels("jc-ssl-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ssl_fail);

            JCDnsSuccess.labels("jc-dns-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_dns_success);
            JCDnsFail.labels("jc-dns-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_dns_fail);

            JCVpnSuccess.labels("jc-vpn-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_vpn_success);
            JCVpnFail.labels("jc-vpn-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_vpn_fail);


            JCMailSuccess.labels("jc-mail-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_mail_success);
            JCMailFail.labels("jc-mail-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_mail_fail);

            JCJstxSuccess.labels("jc-jstx-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_jstx_success);
            JCJstxFail.labels("jc-jstx-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_jstx_fail);

            JCFtpSuccess.labels("jc-ftp-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ftp_success);
            JCFtpFail.labels("jc-ftp-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_ftp_fail);

            JCWygjcSuccess.labels("jc-wygjc-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_wygjc_success);
            JCWygjcFail.labels("jc-wygjc-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_wygjc_fail);

            JCPicSuccess.labels("jc-pic-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_pic_success);
            JCPicFail.labels("jc-pic-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_pic_fail);

            JCMediaSuccess.labels("jc-media-success", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_media_success);
            JCMediaFail.labels("jc-media-fail", String.valueOf(StreamAggregateConfig.COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS / 60)).set(jc_media_fail);
            try {
                push();
            } catch (IOException e) {
                e.printStackTrace();
            }

            jcCount= 0L;
            jcSendCount = 0L;
            jcSendFail = 0L;
            jcParseFail = 0L;
            jc_mail_success=0L;
            jc_mail_fail=0L;
            jc_vpn_success=0L;
            jc_vpn_fail=0L;
            jc_jstx_success=0L;
            jc_jstx_fail=0L;
            jc_ftp_success=0L;
            jc_ftp_fail=0L;
            jc_wygjc_success=0L;
            jc_wygjc_fail=0L;
            jc_pic_success=0L;
            jc_pic_fail=0L;
            jc_media_success=0L;
            jc_media_fail=0L;
            jc_ip_success=0L;
            jc_ip_fail=0L;
            jc_dns_success=0L;
            jc_dns_fail=0L;
            jc_http_success=0L;
            jc_http_fail=0L;
            jc_ssl_success=0L;
            jc_ssl_fail=0L;

        }else {
            String sourceComponent = tuple.getSourceComponent();

            switch (sourceComponent){
                case "JC-PARSE":
                    if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_COUNT_STREAMID)){
                        jcCount = jcCount + tuple.getLongByField("count");
                        jcParseFail = jcParseFail + tuple.getLongByField("loadfail");
                    }
                    break;
                case "JC-SEND":
                    jc_dns_success = jc_dns_success + tuple.getLongByField("jc_dns_success");
                    jc_dns_fail = jc_dns_fail + tuple.getLongByField("jc_dns_fail");

                    jc_mail_success = jc_mail_success + tuple.getLongByField("jc_mail_success");
                    jc_mail_fail = jc_mail_fail + tuple.getLongByField("jc_mail_fail");

                    jc_vpn_success = jc_vpn_success + tuple.getLongByField("jc_vpn_success");
                    jc_vpn_fail = jc_vpn_fail + tuple.getLongByField("jc_vpn_fail");

                    jc_jstx_success = jc_jstx_success + tuple.getLongByField("jc_jstx_success");
                    jc_jstx_fail = jc_jstx_fail + tuple.getLongByField("jc_jstx_fail");


                    jc_ftp_success = jc_ftp_success + tuple.getLongByField("jc_ftp_success");
                    jc_ftp_fail = jc_ftp_fail + tuple.getLongByField("jc_ftp_fail");

                    jc_wygjc_success = jc_wygjc_success + tuple.getLongByField("jc_wygjc_success");
                    jc_wygjc_fail = jc_wygjc_fail + tuple.getLongByField("jc_wygjc_fail");

                    jc_pic_success = jc_pic_success + tuple.getLongByField("jc_pic_success");
                    jc_pic_fail = jc_pic_fail + tuple.getLongByField("jc_pic_fail");

                    jc_media_success = jc_media_success + tuple.getLongByField("jc_media_success");
                    jc_media_fail = jc_media_fail + tuple.getLongByField("jc_media_fail");
                    break;
                case "JC-IP-SEND":
                    jc_ip_success = jc_ip_success + tuple.getLongByField("jc_ip_success");
                    jc_ip_fail = jc_ip_fail + tuple.getLongByField("jc_ip_fail");
                    break;

                case "JC-HTTP-SEND":
                    jc_http_success = jc_http_success + tuple.getLongByField("jc_http_success");
                    jc_http_fail = jc_http_fail + tuple.getLongByField("jc_http_fail");
                    break;
                case "JC-SSL-SEND":
                    jc_ssl_success = jc_ssl_success + tuple.getLongByField("jc_ssl_success");
                    jc_ssl_fail = jc_ssl_fail + tuple.getLongByField("jc_ssl_fail");
                    break;
                default:break;

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
