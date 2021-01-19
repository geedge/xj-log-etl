package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class ConnIPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnIPSendBolt.class);


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

          if(tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_IP_STREAMID)) {
              byte[] ipSource = (byte[]) tuple.getValue(0);
               HttpClientService.getConnHttpPost(getPostUrl(), ipSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_IP_TOPIC);
          }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
