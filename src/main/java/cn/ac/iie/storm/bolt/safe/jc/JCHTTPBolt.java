package cn.ac.iie.storm.bolt.safe.jc;

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
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class JCHTTPBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(JCHTTPBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> httpList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long httpcount = 0L;
    private Long httploadfail = 0L;
    private long httpdatasize = 0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_JCZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!httpList.isEmpty()){             //定时
            httpdatasize = httpdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(httpcount,httploadfail,httpdatasize));

            httpList.clear();
            httpdatasize = 0L;
            httpcount = 0L;
            httploadfail = 0L;
        }else if(httpList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批
            httpdatasize = httpdatasize + getValidLength(generate);
            httpList.clear();
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_HTTP_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord http_gkzb = new GenericData.Record(schema);
                    setHTTP_JCZB(http_gkzb, sessionRecordLog);
                    httpList.add(http_gkzb);
                    generate = generate(schema, httpList);
                    httpcount++;
                } catch (Exception e) {
                    logger.error("http_jczb数据封装处理异常"+e);
                    httploadfail++;

                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.JC_HTTPCOUNT_FIELD,StreamAggregateConfig.JC_HTTPLOADFAIL_FIELD,StreamAggregateConfig.JC_HTTPDATASIZE_FIELD));
    }
    private void setHTTP_JCZB(GenericRecord http_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        http_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        http_jczb.put("PZID",sessionRecordLog.getPzid());
        http_jczb.put("FLID",sessionRecordLog.getFlid());
        http_jczb.put("XZID",sessionRecordLog.getXzid());
        http_jczb.put("LHID",sessionRecordLog.getLhid());
        http_jczb.put("PZLX",sessionRecordLog.getPzlx());
        http_jczb.put("GKLB",sessionRecordLog.getGklb());
        http_jczb.put("yl3",sessionRecordLog.getLabels());
        http_jczb.put("YDY",sessionRecordLog.getCommon_client_location());
        http_jczb.put("MDDY",sessionRecordLog.getCommon_server_location());
        http_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        http_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        http_jczb.put("DETAIL_ID","1"); //同上
        http_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_jczb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        http_jczb.put("yl1",sessionRecordLog.getCommon_address_type());
     //dns特有
        http_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        http_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        http_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        http_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        http_jczb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        http_jczb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
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
