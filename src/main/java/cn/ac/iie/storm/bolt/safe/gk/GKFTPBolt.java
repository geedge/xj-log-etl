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

public class GKFTPBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKFTPBolt.class);
    private static final long serialVersionUID = 4909453288066034335L;
    private List<GenericRecord> ftpList = new ArrayList<GenericRecord>();

    private Schema schema;
    private long ftpcount;
    private long ftpLoadFail = 0L;
    private long ftpdatasize = 0L;
    private SessionRecordLog sessionRecordLog;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.FTP_GKZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!ftpList.isEmpty()){      //定时
         //   httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            ftpdatasize = ftpdatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(ftpcount,ftpLoadFail,ftpdatasize));


            ftpList.clear();
            ftpdatasize =0L;
            ftpcount = 0L;
            ftpLoadFail = 0L;
            generate = null;
        }else if(ftpList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){       //分批
        //    httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate(schema, ftpList),StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            ftpdatasize = ftpdatasize + getValidLength(generate);
            ftpList.clear();
            generate = null;
        }
        else {
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_FTP_STREAMID)) {
                    sessionRecordLog = (SessionRecordLog) tuple.getValue(0);
                }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord ftp_rzzb = new GenericData.Record(schema);
                    setFTP_GKZB(ftp_rzzb, sessionRecordLog);
                    ftpList.add(ftp_rzzb);
                    generate=generate(schema, ftpList);
                    ftpcount++;
                } catch (Exception e) {
                    ftpLoadFail++;
                    logger.error("ftp_gkzb数据封装处理异常"+e);
                }
            }
            }
}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.GK_FTPCOUNT_FIELD,StreamAggregateConfig.GK_FTPLOADFAIL_FIELD,StreamAggregateConfig.GK_FTPDATASIZE_FIELD));

    }
    private void setFTP_GKZB(GenericRecord ftp_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_gkzb.put("PZID",sessionRecordLog.getPzid());
        ftp_gkzb.put("FLID",sessionRecordLog.getFlid());
        ftp_gkzb.put("XZID",sessionRecordLog.getXzid());
        ftp_gkzb.put("LHID",sessionRecordLog.getLhid());
        ftp_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        ftp_gkzb.put("GKLB",sessionRecordLog.getGklb());
        ftp_gkzb.put("yl3",sessionRecordLog.getLabels());
        ftp_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ftp_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ftp_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ftp_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ftp_gkzb.put("DETAIL_ID","1"); //同上
        ftp_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ftp_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        ftp_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //ftp特有
        ftp_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ftp_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ftp_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ftp_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ftp_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_gkzb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
