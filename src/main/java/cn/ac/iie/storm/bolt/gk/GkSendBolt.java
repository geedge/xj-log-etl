package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import java.util.Map;


public class GkSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkSendBolt.class);

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        byte[] dnsSource = (byte[]) tuple.getValue(0);
        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_DNS_TOPIC);
        System.out.println(stringStringMap);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
