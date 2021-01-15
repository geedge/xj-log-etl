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

public class JCWYGJCBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(JCWYGJCBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> wygjcList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long wygjccount = 0L;
    private long wygjcloadfail = 0L;
    private long maildatasize = 0L;
    private byte[] generate = null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.WYGJZ_JCZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!wygjcList.isEmpty()){             //定时
            maildatasize = maildatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(wygjccount,wygjcloadfail,maildatasize));

            wygjcList.clear();
            maildatasize = 0L;
            wygjccount = 0L;
            wygjcloadfail = 0L;
            generate = null;
        }else if(wygjcList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批
            maildatasize = maildatasize + getValidLength(generate);
            wygjcList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_WYGJC_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord dns_gkzb = new GenericData.Record(schema);
                    setWYGJC_JCZB(dns_gkzb, sessionRecordLog);
                    wygjcList.add(dns_gkzb);
                    generate = generate(schema, wygjcList);
                    wygjccount++;
                } catch (Exception e) {
                    logger.error("wygjc_jczb数据封装处理异常"+e);
                    wygjcloadfail++;
                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.JC_WYGJCCOUNT_FIELD,StreamAggregateConfig.JC_WYGJCLOADFAIL_FIELD,StreamAggregateConfig.JC_WYGJCDATASIZE_FIELD));
    }
    private void setWYGJC_JCZB(GenericRecord wygjc_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        wygjc_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        wygjc_jczb.put("PZID",sessionRecordLog.getPzid());
        wygjc_jczb.put("FLID",sessionRecordLog.getFlid());
        wygjc_jczb.put("XZID",sessionRecordLog.getXzid());
        wygjc_jczb.put("LHID",sessionRecordLog.getLhid());
        wygjc_jczb.put("PZLX",sessionRecordLog.getPzlx());
        wygjc_jczb.put("GKLB",sessionRecordLog.getGklb());
        wygjc_jczb.put("yl3",sessionRecordLog.getLabels());
        wygjc_jczb.put("YDY",sessionRecordLog.getCommon_client_location());
        wygjc_jczb.put("MDDY",sessionRecordLog.getCommon_server_location());
        wygjc_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        wygjc_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        wygjc_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        wygjc_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        wygjc_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        wygjc_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        wygjc_jczb.put("DETAIL_ID","1"); //同上
        wygjc_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        wygjc_jczb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        wygjc_jczb.put("yl1",sessionRecordLog.getCommon_address_type());
        //wygjc特有
        wygjc_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        wygjc_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        wygjc_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        wygjc_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        wygjc_jczb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        wygjc_jczb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
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
