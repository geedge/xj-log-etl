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

public class GKIPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKIPSendBolt.class);


    private long gk_ip_success=0L;
    private long gk_ip_fail=0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){
            basicOutputCollector.emit(new Values(gk_ip_success,gk_ip_fail));
            gk_ip_fail = 0L;
            gk_ip_success = 0L;
        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_IP_STREAMID)) {
                byte[] ipSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.GK_IP_STREAMID);
                Integer ipCount = tuple.getIntegerByField("ipcount");
                Map<String, String> gkIpPost =null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    gkIpPost = HttpClientService.getConnHttpPost(postUrl, ipSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_IP_TOPIC);
                    if (gkIpPost.containsValue("200")){
                        logger.info("-------条数为:"+ipCount+"gkip数据发送到"+postUrl+"成功-----"+gkIpPost);
                        gk_ip_success = gk_ip_success + ipCount;
                        break;
                    }
                    i++;
                }
                assert gkIpPost != null;
                if (!gkIpPost.containsValue("200")){
                    logger.error("-------条数为:"+ipCount+"gkip数据发送到"+postUrl+"失败-----"+gkIpPost);
                    gk_ip_fail = gk_ip_fail + ipCount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("gk_ip_success","gk_ip_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
