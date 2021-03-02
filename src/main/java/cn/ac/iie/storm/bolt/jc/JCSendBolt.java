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

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class JCSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCSendBolt.class);


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


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)) {

            basicOutputCollector.emit(new Values(jc_mail_success,jc_mail_fail,jc_vpn_success,jc_vpn_fail,
                    jc_jstx_success,jc_jstx_fail,jc_ftp_success,jc_ftp_fail,jc_wygjc_success,jc_wygjc_fail,jc_pic_success,jc_pic_fail,jc_media_success,jc_media_fail));
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
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MAIL_STREAMID)) {

                    byte[] mailSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_MAIL_STREAMID);
                    Integer mailcount = tuple.getIntegerByField("mailcount");
                    Map<String, String> jcMailPost = HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MAIL_TOPIC);
                    if (jcMailPost.containsValue("200")){
                        logger.info("-------条数为:"+mailcount+"jcmail数据发送成功-----"+jcMailPost);
                        jc_mail_success = jc_mail_success + mailcount;
                    }else {
                        logger.error("-------条数为:"+mailcount+"jcmail数据发送失败-----"+jcMailPost);
                        jc_mail_fail = jc_mail_fail + mailcount;
                    }

                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_VPN_STREAMID)) {

                    byte[] vpnSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_VPN_STREAMID);
                    Integer vpncount = tuple.getIntegerByField("vpncount");
                    Map<String, String> jcVpnPost = HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_VPN_TOPIC);
                    if (jcVpnPost.containsValue("200")){
                        logger.info("-------条数为:"+vpncount+"jcvpn数据发送成功-----"+jcVpnPost);
                        jc_vpn_success = jc_vpn_success + vpncount;
                    }else {
                        logger.error("-------条数为:"+vpncount+"jcvpn数据发送失败-----"+jcVpnPost);
                        jc_vpn_fail = jc_vpn_fail + vpncount;
                    }

                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_JSTX_STREAMID)) {

                    byte[] jstxSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_JSTX_STREAMID);
                    Integer jstxcount = tuple.getIntegerByField("jstxcount");
                    Map<String, String> jcJstxPost = HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_JSTX_TOPIC);
                    if (jcJstxPost.containsValue("200")){
                        logger.info("-------条数为:"+jstxcount+"jcjstx数据发送成功-----"+jcJstxPost);
                        jc_jstx_success = jc_jstx_success + jstxcount;
                    }else {
                        logger.error("-------条数为:"+jstxcount+"jcjstx数据发送失败-----"+jcJstxPost);
                        jc_jstx_fail = jc_jstx_fail + jstxcount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_FTP_STREAMID)) {

                    byte[] ftpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_FTP_STREAMID);
                    Integer ftpcount = tuple.getIntegerByField("ftpcount");
                    Map<String, String> jcFtpPost = HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_FTP_TOPIC);
                    if (jcFtpPost.containsValue("200")){
                        logger.info("-------条数为:"+ftpcount+"jcftp数据发送成功-----"+jcFtpPost);
                        jc_ftp_success = jc_ftp_success + ftpcount;
                    }else {
                        logger.error("-------条数为:"+ftpcount+"jcftp数据发送失败-----"+jcFtpPost);
                        jc_ftp_fail = jc_ftp_fail + ftpcount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_PIC_STREAMID)) {

                    byte[] picSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_PIC_STREAMID);
                    Integer piccount = tuple.getIntegerByField("piccount");
                    Map<String, String> jcPicPost = HttpClientService.getConnHttpPost(getPostUrl(), picSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_PIC_TOPIC);
                    if (jcPicPost.containsValue("200")){
                        logger.info("-------条数为:"+piccount+"jcpic数据发送成功-----"+jcPicPost);
                        jc_pic_success = jc_pic_success + piccount;
                    }else {
                        logger.error("-------条数为:"+piccount+"jcpic数据发送失败-----"+jcPicPost);
                        jc_pic_fail = jc_pic_fail + piccount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_WYGJC_STREAMID)) {
                    byte[] wygjcSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_WYGJC_STREAMID);
                    Integer wygjccount = tuple.getIntegerByField("wygjccount");
                    Map<String, String> jsWygjcPost = HttpClientService.getConnHttpPost(getPostUrl(), wygjcSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_WYGJC_TOPIC);
                    if (jsWygjcPost.containsValue("200")){
                        logger.info("-------条数为:"+wygjccount+"jcwygjc数据发送成功-----"+jsWygjcPost);
                        jc_wygjc_success = jc_wygjc_success + wygjccount;
                    }else {
                        logger.error("-------条数为:"+wygjccount+"jcwygjc数据发送失败-----"+jsWygjcPost);
                        jc_wygjc_fail = jc_wygjc_fail + wygjccount;
                    }
                } else if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MEDIA_STREAMID)) {

                    byte[] mediaSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_MEDIA_STREAMID);
                    Integer mediacount = tuple.getIntegerByField("mediacount");
                    Map<String, String> jcMediaPost = HttpClientService.getConnHttpPost(getPostUrl(), mediaSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MEDIA_TOPIC);
                    if (jcMediaPost.containsValue("200")){
                        logger.info("-------条数为:"+mediacount+"jcmedia数据发送成功-----"+jcMediaPost);
                        jc_media_success = jc_media_success + mediacount;
                    }else {
                        logger.error("-------条数为:"+mediacount+"jcmedia数据发送失败-----"+jcMediaPost);
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
        outputFieldsDeclarer.declare(new Fields("jc_mail_success","jc_mail_fail","jc_vpn_success","jc_vpn_fail",
                "jc_jstx_success","jc_jstx_fail","jc_ftp_success","jc_ftp_fail","jc_wygjc_success","jc_wygjc_fail","jc_pic_success","jc_pic_fail"
                ,"jc_media_success","jc_media_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
