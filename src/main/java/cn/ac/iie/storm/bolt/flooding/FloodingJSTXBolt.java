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
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class FloodingJSTXBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingJSTXBolt.class);
    private static final long serialVersionUID = 1998559489020598047L;
    private List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();
    private Schema schema;
    private Long jstxcount = 0L;
    private Long jstxloadfail = 0L;
    private SessionRecordLog sessionRecordLog;
    private byte[] generate;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            schema = new Schema.Parser().parse(new File("src/main/resources/floodingschema/JSTX_RZZB.avsc"));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!JSTXList.isEmpty()){
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            JSTXList.clear();
            jstxcount = 0L;
            jstxloadfail = 0L;
        }else if(JSTXList.size()==1){
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            JSTXList.clear();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_JSTX_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                jstxcount++;
                try {
                    GenericRecord jstx_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(jstx_rzzb, sessionRecordLog);
                    JSTXList.add(jstx_rzzb);
                     generate = generate(schema, JSTXList);
                  System.out.println(generate);
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }

}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    private void setIP_RZZB(GenericRecord jstx_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_rzzb.put("YDY","ff");
        jstx_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        jstx_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        jstx_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));
        //字段类型不一致，还未处理
        jstx_rzzb.put("DETAIL_ID","1"); //还没处理
        jstx_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_rzzb.put("YYS",1); //-**common_isp这个字段
        jstx_rzzb.put("yl1",sessionRecordLog.getCommon_address_type());
        jstx_rzzb.put("yl2",1);
        jstx_rzzb.put("yl3","null");
        jstx_rzzb.put("yl4","null");
        jstx_rzzb.put("yl5","null");
     //http特有
        jstx_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
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
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
