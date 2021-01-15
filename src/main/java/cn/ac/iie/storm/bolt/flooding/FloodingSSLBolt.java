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
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getFloodingPostUrl;

public class FloodingSSLBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingSSLBolt.class);
    private static final long serialVersionUID = 5732646453844202206L;
    private List<GenericRecord> SSLList = new ArrayList<GenericRecord>();
    private Schema schema;
    private long sslcount = 0L;
    private long sslloadfail = 0L;
    private long ssldatasize = 0L;
    private SessionRecordLog sessionRecordLog;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.SSL_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!SSLList.isEmpty())||SSLList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema, SSLList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_SSL_TOPIC);
            ssldatasize = ssldatasize + getValidLength(generate(schema, SSLList));
            SSLList.clear();
            if(TupleUtils.isTick(tuple)){
                //向下发送指标数据
                basicOutputCollector.emit(new Values(sslcount,sslloadfail,ssldatasize));
                ssldatasize = 0L;
                sslcount = 0L;
                sslloadfail = 0L;
            }
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_HTTP_STREAMID)){
                String line =tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord ssl_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(ssl_rzzb, sessionRecordLog);
                    SSLList.add(ssl_rzzb);
                    sslcount++;
                } catch (Exception e) {
                    logger.error("ssl_rzzb数据封装处理异常"+e);
                    sslloadfail++;
                }
            }
        }
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_SSLCOUNT_FIELD,StreamAggregateConfig.FLOODING_SSLLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_SSLDATASIZE_FIELD));

    }
    private void setIP_RZZB(GenericRecord ssl_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        ssl_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ssl_rzzb.put("YDY","null");  //sessionRecordLog.getCommon_client_location()
        ssl_rzzb.put("MDDY","null"); //sessionRecordLog.getCommon_server_location()
        ssl_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ssl_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ssl_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ssl_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ssl_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ssl_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        ssl_rzzb.put("DETAIL_ID","1"); //同上
        ssl_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ssl_rzzb.put("YYS",0); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        ssl_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());

     //dns特有
        ssl_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ssl_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ssl_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ssl_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
