package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.bolt.gk.GKHTTPSendBolt;
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
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class JCHTTPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCHTTPSendBolt.class);


    private long jc_http_success=0L;
    private long jc_http_fail=0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){

            basicOutputCollector.emit(new Values(jc_http_success,jc_http_fail));
            jc_http_fail = 0L;
            jc_http_success = 0L;

        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_HTTP_STREAMID)) {
                byte[] httpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_HTTP_STREAMID);
                Integer httpcount = tuple.getIntegerByField("httpcount");
                Map<String, String> jcHttpPost = null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    jcHttpPost = HttpClientService.getConnHttpPost(postUrl, httpSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_HTTP_TOPIC);
                    if (jcHttpPost.containsValue("200")){
                        logger.info("-------条数为:"+httpcount+"jchttp数据发送到"+postUrl+"成功-----"+jcHttpPost);
                        jc_http_success = jc_http_success + httpcount;
                        break;
                    }
                    i++;
                }
                assert jcHttpPost != null;
                if (!jcHttpPost.containsValue("200")){
                    logger.error("-------条数为:"+httpcount+"jchttp数据发送到"+postUrl+"失败-----"+jcHttpPost);
                    jc_http_fail = jc_http_fail + httpcount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("jc_http_success","jc_http_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
