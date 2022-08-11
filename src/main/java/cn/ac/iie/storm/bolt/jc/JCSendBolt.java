package cn.ac.iie.storm.bolt.jc;


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

import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class JCSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCSendBolt.class);

    private long jc_dns_success=0L;
    private long jc_dns_fail=0L;

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

    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)) {

            basicOutputCollector.emit(new Values(jc_dns_success,jc_dns_fail,jc_mail_success,jc_mail_fail,jc_vpn_success,jc_vpn_fail,
                    jc_jstx_success,jc_jstx_fail,jc_ftp_success,jc_ftp_fail,jc_wygjc_success,jc_wygjc_fail,jc_pic_success,jc_pic_fail,jc_media_success,jc_media_fail));

            jc_dns_fail = 0L;
            jc_dns_success = 0L;
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
        }else {
            try {
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_DNS_STREAMID)) {
                    byte[] dnsSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_DNS_STREAMID);
                    Integer dnscount = tuple.getIntegerByField("dnscount");
                    Map<String, String> jcDnsPost =null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcDnsPost = HttpClientService.getConnHttpPost(postUrl, dnsSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_DNS_TOPIC);
                        if (jcDnsPost.containsValue("200")){
                            logger.info("-------条数为:" + dnscount + "jcdns数据发送到"+postUrl+"成功-----" + jcDnsPost);
                            jc_dns_success = jc_dns_success + dnscount;
                            break;
                        }
                        i++;
                    }
                    assert jcDnsPost != null;
                    if (!jcDnsPost.containsValue("200")){
                        logger.error("-------条数为:" + dnscount + "jcdns数据发送到"+postUrl+"失败-----" + jcDnsPost);
                        jc_dns_fail = jc_dns_fail + dnscount;
                    }
                }
               else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MAIL_STREAMID)) {
                    byte[] mailSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_MAIL_STREAMID);
                    Integer mailcount = tuple.getIntegerByField("mailcount");
                    Map<String, String> jcMailPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcMailPost = HttpClientService.getConnHttpPost(postUrl, mailSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MAIL_TOPIC);
                        if (jcMailPost.containsValue("200")){
                            logger.info("-------条数为:"+mailcount+"jcmail数据发送到"+postUrl+"成功-----"+jcMailPost);
                            jc_mail_success = jc_mail_success + mailcount;
                            break;
                        }
                        i++;
                    }
                    assert jcMailPost != null;
                    if (!jcMailPost.containsValue("200")){
                        logger.error("-------条数为:"+mailcount+"jcmail数据发送到"+postUrl+"失败-----"+jcMailPost);
                        jc_mail_fail = jc_mail_fail + mailcount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_VPN_STREAMID)) {
                    byte[] vpnSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_VPN_STREAMID);
                    Integer vpncount = tuple.getIntegerByField("vpncount");
                    Map<String, String> jcVpnPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcVpnPost = HttpClientService.getConnHttpPost(postUrl, vpnSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_VPN_TOPIC);
                        if (jcVpnPost.containsValue("200")){
                            logger.info("-------条数为:"+vpncount+"jcvpn数据发送到"+postUrl+"成功-----"+jcVpnPost);
                            jc_vpn_success = jc_vpn_success + vpncount;
                            break;
                        }
                        i++;
                    }
                    assert jcVpnPost != null;
                    if (!jcVpnPost.containsValue("200")){
                        logger.error("-------条数为:"+vpncount+"jcvpn数据发送到"+postUrl+"失败-----"+jcVpnPost);
                        jc_vpn_fail = jc_vpn_fail + vpncount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_JSTX_STREAMID)) {
                    byte[] jstxSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_JSTX_STREAMID);
                    Integer jstxcount = tuple.getIntegerByField("jstxcount");
                    Map<String, String> jcJstxPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcJstxPost = HttpClientService.getConnHttpPost(postUrl, jstxSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_JSTX_TOPIC);
                        if (jcJstxPost.containsValue("200")){
                            logger.info("-------条数为:"+jstxcount+"jcjstx数据发送到"+postUrl+"成功-----"+jcJstxPost);
                            jc_jstx_success = jc_jstx_success + jstxcount;
                            break;
                        }
                        i++;
                    }
                    assert jcJstxPost != null;
                    if (!jcJstxPost.containsValue("200")){
                        logger.error("-------条数为:"+jstxcount+"jcjstx数据发送到"+postUrl+"失败-----"+jcJstxPost);
                        jc_jstx_fail = jc_jstx_fail + jstxcount;
                    }

                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_FTP_STREAMID)) {
                    byte[] ftpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_FTP_STREAMID);
                    Integer ftpcount = tuple.getIntegerByField("ftpcount");
                    Map<String, String> jcFtpPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcFtpPost = HttpClientService.getConnHttpPost(postUrl, ftpSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_FTP_TOPIC);
                        if (jcFtpPost.containsValue("200")){
                            logger.info("-------条数为:"+ftpcount+"jcftp数据发送到"+postUrl+"成功-----"+jcFtpPost);
                            jc_ftp_success = jc_ftp_success + ftpcount;
                            break;
                        }
                        i++;
                    }
                    assert jcFtpPost != null;
                    if (!jcFtpPost.containsValue("200")){
                        logger.error("-------条数为:"+ftpcount+"jcftp数据发送到"+postUrl+"失败-----"+jcFtpPost);
                        jc_ftp_fail = jc_ftp_fail + ftpcount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_PIC_STREAMID)) {
                    byte[] picSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_PIC_STREAMID);
                    Integer piccount = tuple.getIntegerByField("piccount");
                    Map<String, String> jcPicPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jcPicPost = HttpClientService.getConnHttpPost(postUrl, picSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_PIC_TOPIC);
                        if (jcPicPost.containsValue("200")){
                            logger.info("-------条数为:"+piccount+"jcpic数据发送到"+postUrl+"成功-----"+jcPicPost);
                            jc_pic_success = jc_pic_success + piccount;
                            break;
                        }
                        i++;
                    }
                    assert jcPicPost != null;
                    if (!jcPicPost.containsValue("200")){
                        logger.error("-------条数为:"+piccount+"jcpic数据发送到"+postUrl+"失败-----"+jcPicPost);
                        jc_pic_fail = jc_pic_fail + piccount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_WYGJC_STREAMID)) {
                    byte[] wygjcSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_WYGJC_STREAMID);
                    Integer wygjccount = tuple.getIntegerByField("wygjccount");
                    Map<String, String> jsWygjcPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        postUrl = getPostUrl();
                        jsWygjcPost = HttpClientService.getConnHttpPost(postUrl, wygjcSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_WYGJC_TOPIC);
                        if (jsWygjcPost.containsValue("200")){
                            logger.info("-------条数为:"+wygjccount+"jcwygjc数据发送到"+postUrl+"成功-----"+jsWygjcPost);
                            jc_wygjc_success = jc_wygjc_success + wygjccount;
                            break;
                        }
                        i++;
                    }
                    assert jsWygjcPost != null;
                    if (!jsWygjcPost.containsValue("200")){
                        logger.error("-------条数为:"+wygjccount+"jcwygjc数据发送到"+postUrl+"失败-----"+jsWygjcPost);
                        jc_wygjc_fail = jc_wygjc_fail + wygjccount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MEDIA_STREAMID)) {
                    byte[] mediaSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_MEDIA_STREAMID);
                    Integer mediacount = tuple.getIntegerByField("mediacount");
                    Map<String, String> jcMediaPost = null;
                    int i=0;
                    while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                        String postUrl = getPostUrl();
                        jcMediaPost = HttpClientService.getConnHttpPost(postUrl, mediaSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MEDIA_TOPIC);
                        if (jcMediaPost.containsValue("200")){
                            logger.info("-------条数为:"+mediacount+"jcmedia数据发送到"+postUrl+"成功-----"+jcMediaPost);
                            jc_media_success = jc_media_success + mediacount;
                            break;
                        }
                        i++;
                    }
                    assert jcMediaPost != null;
                    if (!jcMediaPost.containsValue("200")){
                        logger.error("-------条数为:"+mediacount+"jcmedia数据发送到"+postUrl+"失败-----"+jcMediaPost);
                        jc_media_fail = jc_media_fail + mediacount;
                    }
                }
            } catch (Exception e) {
                logger.error("批次数据发后端发送异常" + e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("jc_dns_success","jc_dns_fail","jc_mail_success","jc_mail_fail","jc_vpn_success","jc_vpn_fail",
                "jc_jstx_success","jc_jstx_fail","jc_ftp_success","jc_ftp_fail","jc_wygjc_success","jc_wygjc_fail","jc_pic_success","jc_pic_fail"
                ,"jc_media_success","jc_media_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
