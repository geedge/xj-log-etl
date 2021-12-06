package cn.ac.iie.storm.bolt.conn;

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


public class ConnIPSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnIPSendBolt.class);
    private long conn_ip_success = 0L;
    private long conn_ip_fail = 0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){
            basicOutputCollector.emit(new Values(conn_ip_success,conn_ip_fail));
            conn_ip_fail = 0L;
            conn_ip_success = 0L;
        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_IP_STREAMID)) {
                byte[] ipSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_IP_STREAMID);
                Integer ipcount = tuple.getIntegerByField("ipcount");
                Map<String, String> connIpPost = null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    connIpPost = HttpClientService.getConnHttpPost(postUrl,ipSource,StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT,StreamAggregateConfig.CONN_POST_IP_TOPIC);
                    if (connIpPost.containsValue("200")){
                        logger.info("-------条数为:"+ipcount+"connip数据发送到"+postUrl+"成功-----"+connIpPost);
                        conn_ip_success = conn_ip_success + ipcount;
                        break;
                    }
                    i++;
                }
                if (connIpPost!=null&&!connIpPost.containsValue("200")){
                    logger.error("-------条数为:"+ipcount+"connip数据发送到"+postUrl+"失败-----"+connIpPost);
                    conn_ip_fail = conn_ip_fail + ipcount;
                }
            }
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("conn_ip_success","conn_ip_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
