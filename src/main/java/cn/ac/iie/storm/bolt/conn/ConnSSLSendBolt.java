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


public class ConnSSLSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnSSLSendBolt.class);
    private long conn_ssl_success = 0L;
    private long conn_ssl_fail = 0L;
    String postUrl;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){
            basicOutputCollector.emit(new Values(conn_ssl_success,conn_ssl_fail));
            conn_ssl_fail = 0L;
            conn_ssl_success = 0L;
        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.CONN_SSL_STREAMID)) {
                byte[] sslSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.CONN_SSL_STREAMID);
                Integer sslcount = tuple.getIntegerByField("sslcount");
                Map<String, String> connSslPost = null;
                int i=0;
                while (i<StreamAggregateConfig.HTTP_SENT_COUNT){
                    postUrl = getPostUrl();
                    connSslPost = HttpClientService.getConnHttpPost(postUrl,sslSource,StreamAggregateConfig.HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.CONN_POST_SSL_TOPIC);
                    if (connSslPost.containsValue("200")){
                        logger.info("-------条数为:"+sslcount+"connssl数据发送到"+postUrl+"成功-----"+connSslPost);
                        conn_ssl_success = conn_ssl_success + sslcount;
                        break;
                    }
                    i++;
                }
                if (connSslPost!=null&&!connSslPost.containsValue("200")){
                    logger.error("-------条数为:"+sslcount+"connssl数据发送到"+postUrl+"失败-----"+connSslPost);
                    conn_ssl_fail = conn_ssl_fail + sslcount;
                }
            }
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("conn_ssl_success","conn_ssl_fail"));
    }
}
