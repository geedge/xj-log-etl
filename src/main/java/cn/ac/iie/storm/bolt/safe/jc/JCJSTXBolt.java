package cn.ac.iie.storm.bolt.safe.jc;

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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class JCJSTXBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCJSTXBolt.class);
    private static final long serialVersionUID = 1998559489020598047L;
    private List<GenericRecord> jstxList = new ArrayList<GenericRecord>();
    private Schema schema;
    private long jstxcount = 0L;
    private long jstxloadfail = 0L;
    private long jstxdatasize = 0L;
    private byte[] generate=null;

    private SessionRecordLog sessionRecordLog;
    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_JCZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!jstxList.isEmpty()){     //定时
         //   httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            jstxdatasize =  jstxdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(jstxcount,jstxloadfail,jstxdatasize));

            jstxList.clear();
            jstxdatasize = 0L;
            jstxcount = 0L;
            jstxloadfail = 0L;
            generate = null;
        }else if(jstxList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){      //分批
        //    httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate(schema, jstxList),StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            jstxdatasize =  jstxdatasize + getValidLength(generate);
            jstxList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.JC_JSTX_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {

                try {
                    GenericRecord jstx_rzzb = new GenericData.Record(schema);
                    setJSTX_JCZB(jstx_rzzb, sessionRecordLog);
                    jstxList.add(jstx_rzzb);
                    generate = generate(schema, jstxList);
                    jstxcount++;

                } catch (Exception e) {
                    jstxloadfail++;
                    logger.error("jstx_jczb数据封装处理异常"+e);
                }
            }
        }

}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.JC_JSTXCOUNT_FIELD,StreamAggregateConfig.JC_JSTXLOADFAIL_FIELD,StreamAggregateConfig.JC_JSTXDATASIZE_FIELD));
    }

    private void setJSTX_JCZB(GenericRecord jstx_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_jczb.put("PZID",sessionRecordLog.getPzid());
        jstx_jczb.put("FLID",sessionRecordLog.getFlid());
        jstx_jczb.put("XZID",sessionRecordLog.getXzid());
        jstx_jczb.put("LHID",sessionRecordLog.getLhid());
        jstx_jczb.put("PZLX",sessionRecordLog.getPzlx());
        jstx_jczb.put("GKLB",sessionRecordLog.getGklb());
        jstx_jczb.put("yl3",sessionRecordLog.getLabels());
        jstx_jczb.put("YDY",sessionRecordLog.getCommon_client_location());
        jstx_jczb.put("MDDY",sessionRecordLog.getCommon_server_location());
        jstx_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        jstx_jczb.put("DETAIL_ID","1"); //同上
        jstx_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_jczb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        jstx_jczb.put("yl1",sessionRecordLog.getCommon_address_type());
     //http特有
        jstx_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_jczb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_jczb.put("URL",sessionRecordLog.getHttp_url());
        jstx_jczb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_jczb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        jstx_jczb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        jstx_jczb.put("CONTENT",sessionRecordLog.getHttp_snapshot());

 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
