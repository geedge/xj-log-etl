package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;


public class GkSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkSendBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_DNS_STREAMID)) {
            byte[] dnsSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_DNS_TOPIC);
            logger.info("------------------------GK—DNS数据已发送------------------");
        }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_MAIL_STREAMID)) {
            byte[] mailSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_MAIL_TOPIC);
            logger.info("------------------------GK—MAIL数据已发送------------------");
        }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_VPN_STREAMID)) {
            byte[] vpnSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_VPN_TOPIC);
            logger.info("------------------------GK—VPN数据已发送------------------");
        }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_JSTX_STREAMID)) {
            byte[] jstxSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_JSTX_TOPIC);
            logger.info("------------------------GK—JSTX数据已发送------------------");
        } else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_FTP_STREAMID)) {
            byte[] ftpSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_FTP_TOPIC);
            logger.info("------------------------GK—FTP数据已发送------------------");
        }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_WYGJC_STREAMID)) {
            byte[] wygjcSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), wygjcSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_WYGJC_TOPIC);
            logger.info("------------------------GK—WYGJC数据已发送------------------");
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
