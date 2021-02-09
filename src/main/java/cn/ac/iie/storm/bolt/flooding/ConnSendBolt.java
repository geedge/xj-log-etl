package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class ConnSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnSendBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

   if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_DNS_STREAMID)) {
            byte[] dnsSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_DNS_TOPIC);
            logger.info("------------------------CONN—DNS数据已发送------------------");
         }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_MAIL_STREAMID)) {
            byte[] mailSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), mailSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_MAIL_TOPIC);
            logger.info("------------------------CONN—MAIL数据已发送------------------");
         }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_VPN_STREAMID)) {
            byte[] vpnSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), vpnSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_VPN_TOPIC);
            logger.info("------------------------CONN—VPN数据已发送------------------");
         }else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_JSTX_STREAMID)) {
            byte[] jstxSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), jstxSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_JSTX_TOPIC);
            logger.info("------------------------CONN—JSTX数据已发送------------------");
        } else if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_FTP_STREAMID)) {
            byte[] ftpSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), ftpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_FTP_TOPIC);
            logger.info("------------------------CONN—FTP数据已发送------------------");
         }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
