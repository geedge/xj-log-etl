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

public class FloodingHTTPBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingHTTPBolt.class);
    private static final long serialVersionUID = -8681397309652358740L;
    private List<GenericRecord> HTTPList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long httpcount = 0L;
    private long httploadfail = 0L;
    private long httpdatasize = 0L;



    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!HTTPList.isEmpty())||HTTPList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema,HTTPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_HTTP_TOPIC);
            logger.info("返回信息:"+stringStringMap);
            httpdatasize = httpdatasize + getValidLength(generate(schema, HTTPList));
            HTTPList.clear();

                //向下发送指标数据
                basicOutputCollector.emit(new Values(httpcount,httploadfail,httpdatasize));
                httpdatasize = 0L;
                httpcount = 0L;
                httploadfail = 0L;

        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_HTTP_STREAMID)){
                String line =tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord http_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(http_rzzb, sessionRecordLog);
                    HTTPList.add(http_rzzb);
                    httpcount++;
                } catch (Exception e) {
                    httploadfail++;
                    logger.error("http_rzzb数据封装处理异常"+e);
                }
            }
        }

}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_HTTPCOUNT_FIELD,StreamAggregateConfig.FLOODING_HTTPLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_HTTPDATASIZE_FIELD));
    }

    private void setIP_RZZB(GenericRecord http_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        http_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        http_rzzb.put("YDY","null"); //sessionRecordLog.getCommon_client_location()
        http_rzzb.put("MDDY","null"); //sessionRecordLog.getCommon_server_location()
        http_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        http_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        http_rzzb.put("DETAIL_ID","1"); //同上
        http_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        http_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //http特有
        http_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        http_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        http_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        http_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        http_rzzb.put("HOST",sessionRecordLog.getHttp_host());
        http_rzzb.put("URL",sessionRecordLog.getHttp_url());
        http_rzzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        http_rzzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        http_rzzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        http_rzzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());

 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
