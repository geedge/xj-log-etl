package cn.ac.iie.storm.bolt.index;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

public class CountBolt extends BaseBasicBolt {
    private Long count = 0L;
    private Long sucesscount = 0L;
    private Long loadfailcount = 0L;
    InetAddress ip=null;
    private String localip;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {

        try {
            ip=ip.getLocalHost();
        } catch (UnknownHostException e) {
            e.printStackTrace();
        }

        String localname=ip.getHostName();
             localip=ip.getHostAddress();


    }

    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
        if((TupleUtils.isTick(input)&&count!=0L)){
            System.out.println("全部数据的总数"+count);
            System.out.println("全部数据的成功数"+sucesscount);
            System.out.println("全部数据的失败数"+loadfailcount);
            System.out.println("本机的ip地址为"+localip);
            count = 0L;
            sucesscount = 0L;
            loadfailcount = 0L;
        }else {
            if (input.getSourceComponent().equals("FloodingHTTPBolt")) {
                Long httpcount = input.getLongByField("httpcount");
                Long httploadfail = input.getLongByField("httploadfail");
                count = count + httpcount;
                loadfailcount = loadfailcount + httploadfail;
                sucesscount = count - loadfailcount;
            } else if (input.getSourceComponent().equals("FloodingIPBolt")) {
                Long ipcount = input.getLongByField("ipcount");
                Long iploadfail = input.getLongByField("iploadfail");
                count = count + ipcount;
                loadfailcount = loadfailcount + iploadfail;
                sucesscount = count - loadfailcount;
            }
        }

    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
