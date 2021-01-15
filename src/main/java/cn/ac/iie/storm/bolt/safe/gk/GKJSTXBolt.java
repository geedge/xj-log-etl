package cn.ac.iie.storm.bolt.safe.gk;

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

public class GKJSTXBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKJSTXBolt.class);
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
        schema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_GKZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!jstxList.isEmpty()){     //定时
           // httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            jstxdatasize =  jstxdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(jstxcount,jstxloadfail,jstxdatasize));

            jstxList.clear();
            jstxdatasize = 0L;
            jstxcount = 0L;
            jstxloadfail = 0L;
            generate = null;
        }else if(jstxList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){      //分批
          //  httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate(schema, jstxList),StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            jstxdatasize =  jstxdatasize + getValidLength(generate);
            jstxList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_JSTX_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {

                try {
                    GenericRecord jstx_rzzb = new GenericData.Record(schema);
                    setJSTX_GKZB(jstx_rzzb, sessionRecordLog);
                    jstxList.add(jstx_rzzb);
                    generate = generate(schema, jstxList);
                    jstxcount++;

                } catch (Exception e) {
                    jstxloadfail++;
                    logger.error("jstx_gkzb数据封装处理异常"+e);
                }
            }
        }

}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.GK_JSTXCOUNT_FIELD,StreamAggregateConfig.GK_JSTXLOADFAIL_FIELD,StreamAggregateConfig.GK_JSTXDATASIZE_FIELD));
    }

    private void setJSTX_GKZB(GenericRecord jstx_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_gkzb.put("PZID",sessionRecordLog.getPzid());
        jstx_gkzb.put("FLID",sessionRecordLog.getFlid());
        jstx_gkzb.put("XZID",sessionRecordLog.getXzid());
        jstx_gkzb.put("LHID",sessionRecordLog.getLhid());
        jstx_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        jstx_gkzb.put("GKLB",sessionRecordLog.getGklb());
        jstx_gkzb.put("yl3",sessionRecordLog.getLabels());
        jstx_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        jstx_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        jstx_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        jstx_gkzb.put("DETAIL_ID","1"); //同上
        jstx_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        jstx_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
     //http特有
        jstx_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_gkzb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_gkzb.put("URL",sessionRecordLog.getHttp_url());
        jstx_gkzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_gkzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        jstx_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        jstx_gkzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());

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
