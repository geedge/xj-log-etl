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

public class GKHTTPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKHTTPSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_HTTP_STREAMID)){
                byte[] httpSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), httpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_HTTP_TOPIC);
                logger.info("-----------------http_gkzb已经发送----------------------");
            }
        } catch(Exception e){
            logger.error("http_gkzb发送异常"+e);
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
