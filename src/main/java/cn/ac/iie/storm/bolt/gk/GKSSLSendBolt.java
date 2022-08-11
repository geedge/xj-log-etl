package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.HashMap;
import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class GKSSLSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKSSLSendBolt.class);


    private long gk_ssl_success=0L;
    private long gk_ssl_fail=0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){
            basicOutputCollector.emit(new Values(gk_ssl_success,gk_ssl_fail));
            gk_ssl_fail = 0L;
            gk_ssl_success = 0L;
        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_SSL_STREAMID)) {
                byte[] sslSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_SSL_STREAMID);
                Integer sslCount = tuple.getIntegerByField("sslcount");
                Map<String, String> gkSslPost = null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    gkSslPost =  HttpClientService.getConnHttpPost(postUrl, sslSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_SSL_TOPIC);
                    if (gkSslPost.containsValue("200")){
                        logger.info("-------条数为:"+sslCount+"gkssl数据发送到"+postUrl+"成功-----"+gkSslPost);
                        gk_ssl_success = gk_ssl_success + sslCount;
                        break;
                    }
                    i++;
                }
                assert gkSslPost != null;
                if (!gkSslPost.containsValue("200")){
                    logger.error("-------条数为:"+sslCount+"gkssl数据发送到"+postUrl+"失败-----"+gkSslPost);
                    gk_ssl_fail = gk_ssl_fail + sslCount;
                }
            }
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gk_ssl_success","gk_ssl_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
