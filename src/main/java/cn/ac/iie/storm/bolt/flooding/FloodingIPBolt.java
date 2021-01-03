package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
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
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class FloodingIPBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingIPBolt.class);
    private static final long serialVersionUID = 4909453288066034335L;
    private List<GenericRecord> IPList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long ipcount = 0L;
    private long iploadfail = 0L;
    private long ipdatasize =0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.IP_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!IPList.isEmpty()&&generate!=null){       //定时
            httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            ipdatasize =  ipdatasize + getValidLength(generate);
            //向下发送指标
            basicOutputCollector.emit(new Values(ipcount,iploadfail,ipdatasize));

            generate=null;
            IPList.clear();
            ipdatasize=0L;
            ipcount = 0L;
            iploadfail = 0L;
        }else if(IPList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM&&generate!=null){  //分批
            httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            ipdatasize =  ipdatasize + getValidLength(generate);
            generate=null;
            IPList.clear();
        }
        else {
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_IP_STREAMID)) {
                    sessionRecordLog = (SessionRecordLog) tuple.getValue(0);
                }
            if (sessionRecordLog != null) {

                try {
                    GenericRecord ip_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(ip_rzzb, sessionRecordLog);
                    IPList.add(ip_rzzb);
                    generate=generate(schema, IPList);
                    ipcount++;
                } catch (Exception e) {
                    logger.error("ip_rzzb数据封装处理异常"+e);
                    iploadfail++;
                }
            }
            }
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_IPCOUNT_FIELD,StreamAggregateConfig.FLOODING_IPLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_IPDATASIZE_FIELD));

    }
    private void setIP_RZZB(GenericRecord IP_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        IP_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        IP_rzzb.put("YDY","ff");
        IP_rzzb.put("MDDY","暂时没有");
        IP_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        IP_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        IP_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        IP_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        IP_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        IP_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));
        //字段类型不一致，还未处理
        IP_rzzb.put("DETAIL_ID","1"); //还没处理
        IP_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        IP_rzzb.put("YYS",1); //-**common_isp这个字段
        IP_rzzb.put("yl1",sessionRecordLog.getCommon_address_type());
     //dns特有
        IP_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        IP_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        IP_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        IP_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
