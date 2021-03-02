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

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class JCDNSSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCDNSSendBolt.class);

    private long jc_dns_success=0L;
    private long jc_dns_fail=0L;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        if(TupleUtils.isTick(tuple)){

            basicOutputCollector.emit(new Values(jc_dns_success,jc_dns_fail));
            jc_dns_fail = 0L;
            jc_dns_success = 0L;

        } else {
            if (tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_DNS_STREAMID)) {

                byte[] dnsSource = (byte[]) tuple.getValueByField(StreamAggregateConfig.JC_DNS_STREAMID);
                Integer dnscount = tuple.getIntegerByField("dnscount");
                Map<String, String> jcDnsPost = HttpClientService.getConnHttpPost(getPostUrl(), dnsSource, StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_DNS_TOPIC);
                if (jcDnsPost.containsValue("200")){
                    logger.info("-------条数为:"+dnscount+"jcdns数据发送成功-----"+jcDnsPost);
                    jc_dns_success = jc_dns_success + dnscount;
                }else {
                    logger.error("-------条数为:"+dnscount+"jcdns数据发送失败-----"+jcDnsPost);
                    jc_dns_fail = jc_dns_fail + dnscount;
                }
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("jc_dns_success","jc_dns_fail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
