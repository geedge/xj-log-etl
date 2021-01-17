package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getPostUrl;

public class FloodingIPBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingIPBolt.class);
    private static final long serialVersionUID = -153736124285370221L;
    private List<GenericRecord> IPList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long ipcount = 0L;
    private long iploadfail = 0L;
    private long ipdatasize =0L;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.IP_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!IPList.isEmpty())||IPList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getPostUrl(), generate(schema, IPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_IP_TOPIC);
            ipdatasize = ipdatasize + getValidLength(generate(schema, IPList));
            System.out.println(stringStringMap);
            logger.info("返回信息"+stringStringMap);
            IPList.clear();

                //向下发送指标数据
                basicOutputCollector.emit(new Values(ipcount,iploadfail,ipdatasize));
                ipdatasize = 0L;
                ipcount = 0L;
                iploadfail = 0L;

        }
        else {
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_IP_STREAMID)) {
                    String line =tuple.getString(0);
                    sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
                }
            if (sessionRecordLog != null) {
                ipcount++;
                try {
                    GenericRecord ip_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(ip_rzzb, sessionRecordLog);
                    IPList.add(ip_rzzb);
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("ip_rzzb数据封装处理异常"+e);
                    iploadfail++;
                }
            }
            }
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_IPCOUNT_FIELD,StreamAggregateConfig.FLOODING_IPLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_IPDATASIZE_FIELD));

    }
    private void setIP_RZZB(GenericRecord IP_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        IP_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        IP_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        IP_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location()); //sessionRecordLog.getCommon_server_location()
        IP_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        IP_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        IP_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        IP_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        IP_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        IP_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        IP_rzzb.put("DETAIL_ID","08003"); //同上
        IP_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        IP_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        IP_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
     //dns特有
        IP_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        IP_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        IP_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        IP_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
