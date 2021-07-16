package cn.ac.iie.storm.bolt.jc;


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

public class JCSSLSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCSSLSendBolt.class);
    private long jc_ssl_success=0L;
    private long jc_ssl_fail=0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){

            basicOutputCollector.emit(new Values(jc_ssl_success,jc_ssl_fail));
            jc_ssl_fail = 0L;
            jc_ssl_success = 0L;

        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_SSL_STREAMID)) {
                byte[] sslSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_SSL_STREAMID);
                Integer sslcount = tuple.getIntegerByField("sslcount");
                Map<String, String> jcSslPost = null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    jcSslPost = HttpClientService.getConnHttpPost(postUrl, sslSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_SSL_TOPIC);
                    if (jcSslPost.containsValue("200")){
                        logger.info("-------条数为:"+sslcount+"jcip数据发送成功-----"+jcSslPost);
                        jc_ssl_success = jc_ssl_success + sslcount;
                        break;
                    }
                    i++;
                }
                assert jcSslPost != null;
                if (!jcSslPost.containsValue("200")){
                    logger.error("-------条数为:"+sslcount+"jcip数据发送失败-----"+jcSslPost);
                    jc_ssl_fail = jc_ssl_fail + sslcount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("jc_ssl_success","jc_ssl_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
