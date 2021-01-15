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

public class JCPICBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(JCPICBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> picList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long piccount = 0L;
    private Long picloadfail = 0L;
    private long picdatasize = 0L;
    private byte[] generate = null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.PIC_JCZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!picList.isEmpty()){             //定时
            picdatasize = picdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(piccount,picloadfail,picdatasize));

            picList.clear();
            picdatasize = 0L;
            piccount = 0L;
            picloadfail = 0L;
            generate = null;
        }else if(picList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批
            picdatasize = picdatasize + getValidLength(generate);
            picList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_PIC_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord dns_gkzb = new GenericData.Record(schema);
                    setPIC_JCZB(dns_gkzb, sessionRecordLog);
                    picList.add(dns_gkzb);
                    generate = generate(schema, picList);
                    piccount++;
                } catch (Exception e) {
                    logger.error("pic_jczb数据封装处理异常"+e);
                    picloadfail++;
                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.JC_PICCOUNT_FIELD,StreamAggregateConfig.JC_PICLOADFAIL_FIELD,StreamAggregateConfig.JC_PICDATASIZE_FIELD));
    }
    private void setPIC_JCZB(GenericRecord pic_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        pic_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        pic_jczb.put("PZID",sessionRecordLog.getPzid());
        pic_jczb.put("FLID",sessionRecordLog.getFlid());
        pic_jczb.put("XZID",sessionRecordLog.getXzid());
        pic_jczb.put("LHID",sessionRecordLog.getLhid());
        pic_jczb.put("PZLX",sessionRecordLog.getPzlx());
        pic_jczb.put("GKLB",sessionRecordLog.getGklb());
        pic_jczb.put("yl3",sessionRecordLog.getLabels());
        pic_jczb.put("YDY",sessionRecordLog.getCommon_client_location());
        pic_jczb.put("MDDY",sessionRecordLog.getCommon_server_location());
        pic_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        pic_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        pic_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        pic_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        pic_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        pic_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        pic_jczb.put("DETAIL_ID","1"); //同上
        pic_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        pic_jczb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        pic_jczb.put("yl1",sessionRecordLog.getCommon_address_type());
        //pic特有
        pic_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        pic_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        pic_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        pic_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        pic_jczb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        pic_jczb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
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
