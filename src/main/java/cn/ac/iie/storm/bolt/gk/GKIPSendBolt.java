package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.bolt.flooding.ConnIPSendBolt;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class GKIPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKIPSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try{
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_IP_STREAMID)) {
                byte[] ipSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), ipSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_IP_TOPIC);
                logger.info("-----------------ip_gkzb已经发送----------------------");
            }
        }
        catch(Exception e){
            logger.error("ip_gkzb发送异常"+e);
        }
        }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
