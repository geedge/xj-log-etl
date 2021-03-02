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

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class GKHTTPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKHTTPSendBolt.class);

    private long gk_http_success=0L;
    private long gk_http_fail=0L;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){

            basicOutputCollector.emit(new Values(gk_http_success,gk_http_fail));
            gk_http_fail = 0L;
            gk_http_success = 0L;

        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_HTTP_STREAMID)) {
                byte[] httpSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_HTTP_STREAMID);
                Integer httpcount = tuple.getIntegerByField("httpcount");
                Map<String, String> gkHttpPost = HttpClientService.getConnHttpPost(getPostUrl(), httpSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_HTTP_TOPIC);
                if (gkHttpPost.containsValue("200")){
                    logger.info("-------条数为:"+httpcount+"gkhttp数据发送成功-----"+gkHttpPost);
                    gk_http_success = gk_http_success + httpcount;
                }else {
                    logger.error("-------条数为:"+httpcount+"gkhttp数据发送失败-----"+gkHttpPost);
                    gk_http_fail = gk_http_fail + httpcount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gk_http_success","gk_http_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
