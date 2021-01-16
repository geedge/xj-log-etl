package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getFloodingPostUrl;

public class FloodingFTPBolt extends BaseBasicBolt {
    private static final Logger logger = Logger.getLogger(FloodingFTPBolt.class);
    private static final long serialVersionUID = -4186799016187928031L;
    private List<GenericRecord> FTPList = new ArrayList<GenericRecord>();

    private Schema schema;
    private long ftpcount;
    private long ftpLoadFail = 0L;
    private long ftpdatasize = 0L;
    private SessionRecordLog sessionRecordLog;



    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.FTP_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!FTPList.isEmpty())||FTPList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema,FTPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_FTP_TOPIC);
            logger.info("返回信息"+stringStringMap);
            ftpdatasize = ftpdatasize + getValidLength(generate(schema, FTPList));
            FTPList.clear();

                //向下发送指标数据
                basicOutputCollector.emit(new Values(ftpcount,ftpLoadFail,ftpdatasize));
                ftpdatasize = 0L;
                ftpcount = 0L;
                ftpLoadFail = 0L;

        }
        else {
                if (tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_FTP_STREAMID)) {
                    String line =tuple.getString(0);
                    sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
                }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord ftp_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(ftp_rzzb, sessionRecordLog);
                    FTPList.add(ftp_rzzb);
                    ftpcount++;
                } catch (Exception e) {
                    ftpLoadFail++;
                    logger.error("ftp_rzzb数据封装处理异常"+e);
                }
            }
            }
}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_FTPCOUNT_FIELD,StreamAggregateConfig.FLOODING_FTPLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_FTPDATASIZE_FIELD));

    }
    private void setIP_RZZB(GenericRecord ftp_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ftp_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ftp_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ftp_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        ftp_rzzb.put("DETAIL_ID","1"); //同上
        ftp_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ftp_rzzb.put("YYS",0); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        ftp_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());

        //ftp特有
        ftp_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ftp_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ftp_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ftp_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ftp_rzzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_rzzb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }

}
