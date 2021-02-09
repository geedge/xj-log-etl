package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.bolt.gk.GkSendBolt;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class JCSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCSendBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MAIL_STREAMID)) {
                byte[] mailSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MAIL_TOPIC);
                logger.info("------------------------JC—MAIL数据已发送------------------");
            }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_VPN_STREAMID)) {
                byte[] vpnSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_VPN_TOPIC);
                logger.info("------------------------JC—VPN数据已发送------------------");
            }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_JSTX_STREAMID)) {
                byte[] jstxSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_JSTX_TOPIC);
                logger.info("------------------------JC—JSTX数据已发送------------------");
            } else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_FTP_STREAMID)) {
                byte[] ftpSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_FTP_TOPIC);
                logger.info("------------------------JC—FTP数据已发送------------------");
            }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_PIC_STREAMID)) {
                byte[] picSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), picSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_PIC_TOPIC);
                logger.info("------------------------JC—PIC数据已发送------------------");
            }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_WYGJC_STREAMID)) {
                byte[] wygjcSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), wygjcSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_WYGJC_TOPIC);
                logger.info("------------------------JC—WYGJC数据已发送------------------");
            }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_MEDIA_STREAMID)) {
                byte[] mediaSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), mediaSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_MEDIA_TOPIC);
                logger.info("------------------------JC—MEDIA数据已发送------------------");
            }
        }catch(Exception e){
            logger.error("批次数据发后端发送异常"+e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
