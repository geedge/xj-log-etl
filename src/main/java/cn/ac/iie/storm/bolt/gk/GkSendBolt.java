package cn.ac.iie.storm.bolt.gk;

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


public class GkSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkSendBolt.class);
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

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if (TupleUtils.isTick(tuple)) {
            basicOutputCollector.emit(new Values(gk_dns_success, gk_dns_fail,gk_mail_success,gk_mail_fail,gk_vpn_success,gk_vpn_fail,
                    gk_jstx_success,gk_jstx_fail,gk_ftp_success,gk_ftp_fail,gk_wygjc_success,gk_wygjc_fail));


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
        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_DNS_STREAMID)) {

                byte[] dnsSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_DNS_STREAMID);
                Integer dnscount = tuple.getIntegerByField("dnscount");
                Map<String, String> gkDnsPost = HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_DNS_TOPIC);
                if (gkDnsPost.containsValue("200")){
                   logger.info("-------条数为:"+dnscount+"gkdns数据发送成功-----"+gkDnsPost);
                   gk_dns_success = gk_dns_success + dnscount;
                    }else {
                    logger.error("-------条数为:"+dnscount+"gkdns数据发送失败-----"+gkDnsPost);
                    gk_dns_fail = gk_dns_fail + dnscount;
                    }
            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_MAIL_STREAMID)) {

                byte[] mailSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_MAIL_STREAMID);
                Integer mailcount = tuple.getIntegerByField("mailcount");
                Map<String, String> gkMailPost = HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_MAIL_TOPIC);
                if (gkMailPost.containsValue("200")){
                    logger.info("-------条数为:"+mailcount+"gkmail数据发送成功-----"+gkMailPost);
                    gk_mail_success = gk_mail_success + mailcount;
                }else {
                    logger.error("-------条数为:"+mailcount+"gkmail数据发送失败-----"+gkMailPost);
                    gk_mail_fail = gk_mail_fail + mailcount;
                }
            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_VPN_STREAMID)) {

                byte[] vpnSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_VPN_STREAMID);
                Integer vpnCount = tuple.getIntegerByField("vpncount");
                Map<String, String> gkVpnPost = HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_VPN_TOPIC);
                if (gkVpnPost.containsValue("200")){
                    logger.info("-------条数为:"+vpnCount+"gkvpn数据发送成功-----"+gkVpnPost);
                    gk_vpn_success = gk_vpn_success + vpnCount;
                }else {
                    logger.error("-------条数为:"+vpnCount+"gkvpn数据发送失败-----"+gkVpnPost);
                    gk_vpn_fail = gk_vpn_fail + vpnCount;
                }
            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_JSTX_STREAMID)) {

                byte[] jstxSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_JSTX_STREAMID);
                Integer jstxCount = tuple.getIntegerByField("jstxcount");
                Map<String, String> gkJstxPost = HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_JSTX_TOPIC);
                if (gkJstxPost.containsValue("200")){
                    logger.info("-------条数为:"+jstxCount+"gkjstx数据发送成功-----"+gkJstxPost);
                    gk_jstx_success = gk_jstx_success + jstxCount;
                }else {
                    logger.error("-------条数为:"+jstxCount+"gkjstx数据发送失败-----"+gkJstxPost);
                    gk_jstx_fail = gk_jstx_fail + jstxCount;
                }

            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_FTP_STREAMID)) {

                byte[] ftpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_FTP_STREAMID);
                Integer ftpCount = tuple.getIntegerByField("ftpcount");
                Map<String, String> gkFtpPost = HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_FTP_TOPIC);
                if (gkFtpPost.containsValue("200")){
                    logger.info("-------条数为:"+ftpCount+"gkftp数据发送成功-----"+gkFtpPost);
                    gk_ftp_success = gk_ftp_success + ftpCount;
                }else {
                    logger.error("-------条数为:"+ftpCount+"gkftp数据发送失败-----"+gkFtpPost);
                    gk_ftp_fail = gk_ftp_fail + ftpCount;
                }
            } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_WYGJC_STREAMID)) {

                byte[] wygjcSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_WYGJC_STREAMID);
                Integer wygjcCount = tuple.getIntegerByField("wygjccount");
                Map<String, String> gkWygjcPost = HttpClientService.getConnHttpPost(getPostUrl(), wygjcSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_WYGJC_TOPIC);
                if (gkWygjcPost.containsValue("200")){
                    logger.info("-------条数为:"+wygjcCount+"gkwygjc数据发送成功-----"+gkWygjcPost);
                    gk_wygjc_success = gk_wygjc_success + wygjcCount;
                }else {
                    logger.error("-------条数为:"+wygjcCount+"gkwygjc数据发送失败-----"+gkWygjcPost);
                    gk_wygjc_fail = gk_wygjc_fail + wygjcCount;
                }

            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gk_dns_success", "gk_dns_fail","gk_mail_success","gk_mail_fail","gk_vpn_success","gk_vpn_fail",
                "gk_jstx_success","gk_jstx_fail","gk_ftp_success","gk_ftp_fail","gk_wygjc_success","gk_wygjc_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
