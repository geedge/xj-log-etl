package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.bolt.gk.GKHTTPSendBolt;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class JCHTTPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCHTTPSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        try {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_HTTP_STREAMID)){
                byte[] httpSource = (byte[]) tuple.getValue(0);
                HttpClientService.getConnHttpPost(getPostUrl(), httpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_HTTP_TOPIC);
                logger.info("-----------------http_jczb已经发送----------------------");
            }
        }catch(Exception e){
            logger.error("http_jczb发送异常"+e);
        }


    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
