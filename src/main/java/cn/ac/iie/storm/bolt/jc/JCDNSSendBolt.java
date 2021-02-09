package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class JCDNSSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCDNSSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_DNS_STREAMID)) {
                byte[] dnsSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_DNS_TOPIC);
                logger.info("-----------------dns_jczb已经发送----------------------");
            }
        } catch(Exception e){
        logger.error("dns_jczb发送异常"+e);
    }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
