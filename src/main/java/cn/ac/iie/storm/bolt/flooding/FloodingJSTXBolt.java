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

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getFloodingPostUrl;

public class FloodingJSTXBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingJSTXBolt.class);
    private static final long serialVersionUID = 2331308412776273908L;
    private List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();
    private Schema schema;
    private long jstxcount = 0L;
    private long jstxloadfail = 0L;
    private long jstxdatasize = 0L;


    private SessionRecordLog sessionRecordLog;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!JSTXList.isEmpty())||JSTXList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema, JSTXList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_JSTX_TOPIC);
            jstxdatasize = jstxdatasize + getValidLength(generate(schema, JSTXList));
            JSTXList.clear();
            if(TupleUtils.isTick(tuple)){
                //向下发送指标数据
                basicOutputCollector.emit(new Values(jstxcount,jstxloadfail,jstxdatasize));
                jstxdatasize = 0L;
                jstxcount = 0L;
                jstxloadfail = 0L;
            }
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_JSTX_STREAMID)){
                String line =tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {

                try {
                    GenericRecord jstx_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(jstx_rzzb, sessionRecordLog);
                    JSTXList.add(jstx_rzzb);
                    jstxcount++;
                } catch (Exception e) {
                    jstxloadfail++;
                    logger.error("jstx_rzzb数据封装处理异常"+e);
                }
            }
        }

}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_JSTXCOUNT_FIELD,StreamAggregateConfig.FLOODING_JSTXLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_JSTXDATASIZE_FIELD));
    }

    private void setIP_RZZB(GenericRecord jstx_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        jstx_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        jstx_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        jstx_rzzb.put("DETAIL_ID","1"); //同上
        jstx_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_rzzb.put("YYS",0); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        jstx_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
     //ftp特有
        jstx_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        jstx_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_rzzb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_rzzb.put("URL",sessionRecordLog.getHttp_url());
        jstx_rzzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_rzzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        jstx_rzzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        jstx_rzzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());

 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
