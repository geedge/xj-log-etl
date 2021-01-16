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

public class FloodingVPNBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingVPNBolt.class);
    private static final long serialVersionUID = -3215806301782539655L;
    private List<GenericRecord> VPNList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long vpncount = 0L;
    private long vpnloadfail = 0L;
    private long vpndatasize = 0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.VPN_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!VPNList.isEmpty())||VPNList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema, VPNList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_VPN_TOPIC);
            vpndatasize = vpndatasize + getValidLength(generate(schema, VPNList));
            VPNList.clear();
            logger.info("返回信息"+stringStringMap);
                //向下发送指标数据
                basicOutputCollector.emit(new Values(vpncount,vpnloadfail,vpndatasize));
                vpndatasize = 0L;
                vpncount = 0L;
                vpnloadfail = 0L;

        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_VPN_STREAMID)){
                String line =tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord vpn_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(vpn_rzzb, sessionRecordLog);
                    VPNList.add(vpn_rzzb);
                    generate = generate(schema, VPNList);
                    vpncount++;
                } catch (Exception e) {
                    logger.error("vpn_rzzb数据封装处理异常"+e);
                    vpnloadfail++;
                }
            }
        }
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_VPNCOUNT_FIELD,StreamAggregateConfig.FLOODING_VPNLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_VPNDATASIZE_FIELD));
    }
    private void setIP_RZZB(GenericRecord vpn_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        vpn_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        vpn_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        vpn_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        vpn_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        vpn_rzzb.put("DETAIL_ID","1"); //同上
        vpn_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        vpn_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        vpn_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //vpn特有
        vpn_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        vpn_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        vpn_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        vpn_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
