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
import static cn.ac.iie.storm.utils.general.TransFormUtils.getValidLength;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getFloodingPostUrl;

public class FloodingDNSBolt extends BaseBasicBolt {
  private  static final  Logger logger = Logger.getLogger(FloodingDNSBolt.class);
    private static final long serialVersionUID = -3137281643694795987L;
    private List<GenericRecord> DNSList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long dnscount = 0L;
    private Long dnsloadfail = 0L;
    private long dnsdatasize = 0L;

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.DNS_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!DNSList.isEmpty())||DNSList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema,DNSList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_DNS_TOPIC);
            logger.info("返回信息"+stringStringMap);
            dnsdatasize = dnsdatasize + getValidLength(generate(schema, DNSList));
            DNSList.clear();

                //向下发送指标数据
                basicOutputCollector.emit(new Values(dnscount,dnsloadfail,dnsdatasize));
                dnsdatasize = 0L;
                dnscount = 0L;
                dnsloadfail = 0L;

        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_HTTP_STREAMID)){
               String line =tuple.getString(0);
               sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord dns_rzzb = new GenericData.Record(schema);
                    setDNS_RZZB(dns_rzzb, sessionRecordLog);
                    DNSList.add(dns_rzzb);
                    dnscount++;
                } catch (Exception e) {
                    logger.error("dns_rzzb数据封装处理异常"+e);
                    dnsloadfail++;
                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_DNSCOUNT_FIELD,StreamAggregateConfig.FLOODING_DNSLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_DNSDATASIZE_FIELD));
    }
    private void setDNS_RZZB(GenericRecord dns_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        dns_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        dns_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        dns_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        dns_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        dns_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        dns_rzzb.put("DETAIL_ID","1"); //同上
        dns_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_rzzb.put("YYS",0); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        dns_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
      //dns特有
        dns_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        dns_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        dns_rzzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        dns_rzzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
