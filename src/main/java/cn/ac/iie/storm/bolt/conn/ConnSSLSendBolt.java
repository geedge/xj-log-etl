package cn.ac.iie.storm.bolt.conn;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class ConnSSLSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnSSLSendBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            byte[] sslSource = (byte[]) tuple.getValue(0);
            HttpClientService.getConnHttpPost(getPostUrl(), sslSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_SSL_TOPIC);
            logger.info("-----------------http_rzzb已经发送----------------------");
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
