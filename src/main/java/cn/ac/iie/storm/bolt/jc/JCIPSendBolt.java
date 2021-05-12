package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.bolt.gk.GKIPSendBolt;
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

public class JCIPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCIPSendBolt.class);


    private long jc_ip_success=0L;
    private long jc_ip_fail=0L;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){

            basicOutputCollector.emit(new Values(jc_ip_success,jc_ip_fail));
            jc_ip_fail = 0L;
            jc_ip_success = 0L;

        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_IP_STREAMID)) {

                byte[] ipSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_IP_STREAMID);
                Integer ipcount = tuple.getIntegerByField("ipcount");
                Map<String, String> jcIpPost = HttpClientService.getConnHttpPost(getPostUrl(), ipSource, StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_IP_TOPIC);
                if (jcIpPost.containsValue("200")){
                    logger.info("-------条数为:"+ipcount+"jcip数据发送成功-----"+jcIpPost);
                    jc_ip_success = jc_ip_success + ipcount;
                }else {
                    logger.error("-------条数为:"+ipcount+"jcip数据发送失败-----"+jcIpPost);
                    jc_ip_fail = jc_ip_fail + ipcount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("jc_ip_success","jc_ip_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
