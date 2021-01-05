package cn.ac.iie.storm.bolt.safe.gk;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumWriter;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class GKSSLBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(GKSSLBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> sslList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long sslcount = 0L;
    private Long sslloadfail = 0L;
    private long ssldatasize = 0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.SSL_GKZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!sslList.isEmpty()){             //定时
            ssldatasize = ssldatasize + getValidLength(generate);

            sslList.clear();
            ssldatasize = 0L;
            sslcount = 0L;
            sslloadfail = 0L;
        }else if(sslList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批
            ssldatasize = ssldatasize + getValidLength(generate);
            sslList.clear();
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_SSL_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord ssl_gkzb = new GenericData.Record(schema);
                    setDNS_RZZB(ssl_gkzb, sessionRecordLog);
                    sslList.add(ssl_gkzb);
                    generate = generate(schema, sslList);
                    sslcount++;
                } catch (Exception e) {
                    logger.error("dns_gkzb数据封装处理异常"+e);
                    sslloadfail++;

                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
    private void setDNS_RZZB(GenericRecord dns_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_gkzb.put("YDY","ff");
        dns_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
       // dns_gkzb.put("QDJIP",sessionRecordLog.getCommon_sled_ip());
        dns_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_gkzb.put("SIP",111);
        dns_gkzb.put("DIP",sessionRecordLog.getCommon_server_ip());
        dns_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
       //字段类型不一致，还未处理
        dns_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        dns_gkzb.put("DETAIL_ID","1"); //同上
        dns_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_gkzb.put("YYS",1); //-**common_isp这个字段
        dns_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
     //dns特有
        dns_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        dns_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        dns_gkzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        dns_gkzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

    private byte[] generate(Schema schema, List<GenericRecord> genericRecordList){
        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>();
        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            dataFileWriter.create(schema, out);
            if (genericRecordList != null && genericRecordList.size() > 0) {
                for (int i = 0; i < genericRecordList.size(); i++) {
                    dataFileWriter.append(genericRecordList.get(i));
                }
            }
            dataFileWriter.flush();
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                dataFileWriter.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
