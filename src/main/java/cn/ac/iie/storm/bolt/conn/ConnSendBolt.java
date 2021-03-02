package cn.ac.iie.storm.bolt.conn;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class ConnSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnSendBolt.class);
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

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (TupleUtils.isTick(tuple)) {

            basicOutputCollector.emit(new Values(conn_dns_success, conn_dns_fail,conn_mail_success,conn_mail_fail,conn_vpn_success,conn_vpn_fail,
                    conn_jstx_success,conn_jstx_fail,conn_ftp_success,conn_ftp_fail));


            conn_dns_success = 0L;
            conn_dns_fail = 0L;
            conn_mail_success=0L;
            conn_mail_fail=0L;
            conn_vpn_success=0L;
            conn_vpn_fail=0L;
            conn_jstx_success=0L;
            conn_jstx_fail=0L;
            conn_ftp_success=0L;
            conn_ftp_fail=0L;
        } else {

            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_DNS_STREAMID)) {

                byte[] dnsSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_DNS_STREAMID);
                Integer dnscount = tuple.getIntegerByField("dnscount");
                Map<String, String> dnsPost = HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_DNS_TOPIC);
                if (dnsPost.containsValue("200")){
                    logger.info("-------条数为:"+dnscount+"conndns数据发送成功-----"+dnsPost);
                    conn_dns_success = conn_dns_success + dnscount;
                }else {
                    logger.error("-------条数为:"+dnscount+"conndns数据发送失败-----"+dnsPost);
                    conn_dns_fail = conn_dns_fail + dnscount;
                }

            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_MAIL_STREAMID)) {

                byte[] mailSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_MAIL_STREAMID);
                Integer mailcount = tuple.getIntegerByField("mailcount");
                Map<String, String> mailPost = HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_MAIL_TOPIC);
                if (mailPost.containsValue("200")){
                    logger.info("-------条数为:"+mailcount+"connmail数据发送成功-----"+mailPost);
                    conn_mail_success = conn_mail_success + mailcount;
                }else {
                    logger.error("-------条数为:"+mailcount+"connmail数据发送失败-----"+mailPost);
                    conn_mail_fail = conn_mail_fail + mailcount;
                }

            }else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_VPN_STREAMID)) {

                byte[] vpnSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_VPN_STREAMID);
                Integer vpncount = tuple.getIntegerByField("vpncount");
                Map<String, String> vpnPost = HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_VPN_TOPIC);
                if (vpnPost.containsValue("200")){
                    logger.info("-------条数为:"+vpncount+"connvpn数据发送成功-----"+vpnPost);
                    conn_vpn_success = conn_vpn_success + vpncount;
                }else {
                    logger.error("-------条数为:"+vpncount+"connvpn数据发送失败-----"+vpnPost);
                    conn_vpn_fail = conn_vpn_fail + vpncount;
                }

            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_JSTX_STREAMID)) {
                byte[] jstxSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_JSTX_STREAMID);
                Integer jstxcount = tuple.getIntegerByField("jstxcount");
                Map<String, String> jstxPost = HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_JSTX_TOPIC);
                if (jstxPost.containsValue("200")){
                    logger.info("-------条数为:"+jstxcount+"connjstx数据发送成功-----"+jstxPost);
                    conn_jstx_success = conn_jstx_success + jstxcount;
                }else {
                    logger.error("-------条数为:"+jstxPost+"connjstx数据发送失败-----"+jstxPost);
                    conn_jstx_fail = conn_jstx_fail + jstxcount;
                }


            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_FTP_STREAMID)) {
                byte[] ftpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_FTP_STREAMID);
                Integer ftpcount = tuple.getIntegerByField("ftpcount");
                Map<String, String> ftpPost = HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_FTP_TOPIC);

                if (ftpPost.containsValue("200")){
                    logger.info("-------条数为:"+ftpcount+"connftp数据发送成功-----"+ftpPost);
                    conn_ftp_success = conn_ftp_success + ftpcount;
                }else {
                    logger.error("-------条数为:"+ftpcount+"connftp数据发送失败-----"+ftpPost);
                    conn_ftp_fail = conn_ftp_fail + ftpcount;
                }

            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {


        outputFieldsDeclarer.declare(new Fields("conn_dns_success", "conn_dns_fail","conn_mail_success","conn_mail_fail","conn_vpn_success","conn_vpn_fail",
                "conn_jstx_success","conn_jstx_fail","conn_ftp_success","conn_ftp_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.CONN_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
