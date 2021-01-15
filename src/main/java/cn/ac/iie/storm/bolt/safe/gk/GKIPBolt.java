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

public class GKIPBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(GKIPBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> ipList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long ipcount = 0L;
    private Long iploadfail = 0L;
    private long ipdatasize = 0L;
    private byte[] generate = null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.IP_GKRZ_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!ipList.isEmpty()){             //定时
            ipdatasize = ipdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(ipcount,iploadfail,ipdatasize));

            ipList.clear();
            ipdatasize = 0L;
            ipcount = 0L;
            iploadfail = 0L;
            generate = null;
        }else if(ipList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批

            ipdatasize = ipdatasize + getValidLength(generate);
            ipList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_IP_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord ip_gkrz = new GenericData.Record(schema);
                    setIP_GKRZ(ip_gkrz, sessionRecordLog);
                    ipList.add(ip_gkrz);
                    generate = generate(schema, ipList);
                    ipcount++;
                } catch (Exception e) {
                    logger.error("ip_gkrz数据封装处理异常"+e);
                    iploadfail++;

                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.GK_IPCOUNT_FIELD,StreamAggregateConfig.GK_IPLOADFAIL_FIELD,StreamAggregateConfig.GK_IPDATASIZE_FIELD));
    }
    private void setIP_GKRZ(GenericRecord ip_gkrz, SessionRecordLog sessionRecordLog) throws IOException {
        ip_gkrz.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ip_gkrz.put("PZID",sessionRecordLog.getPzid());
        ip_gkrz.put("FLID",sessionRecordLog.getFlid());
        ip_gkrz.put("XZID",sessionRecordLog.getXzid());
        ip_gkrz.put("LHID",sessionRecordLog.getLhid());
        ip_gkrz.put("PZLX",sessionRecordLog.getPzlx());
        ip_gkrz.put("GKLB",sessionRecordLog.getGklb());
        ip_gkrz.put("yl3",sessionRecordLog.getLabels());
        ip_gkrz.put("YDY",sessionRecordLog.getCommon_client_location());
        ip_gkrz.put("MDDY",sessionRecordLog.getCommon_server_location());
        ip_gkrz.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ip_gkrz.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ip_gkrz.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ip_gkrz.put("SPORT",sessionRecordLog.getCommon_client_port());
        ip_gkrz.put("DPORT",sessionRecordLog.getCommon_server_port());
        ip_gkrz.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ip_gkrz.put("DETAIL_ID","1"); //同上
        ip_gkrz.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ip_gkrz.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        ip_gkrz.put("yl1",sessionRecordLog.getCommon_address_type());

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
