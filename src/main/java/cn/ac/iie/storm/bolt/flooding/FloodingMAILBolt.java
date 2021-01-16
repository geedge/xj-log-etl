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

import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getFloodingPostUrl;

public class FloodingMAILBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingMAILBolt.class);
    private static final long serialVersionUID = -2036557232577300324L;
    private List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
    private Schema schema;
    private long mailcount;
    private long mailloadfail;
    private long maildatasize;
    private SessionRecordLog sessionRecordLog;




    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_RZZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        HttpClientService httpClientService = new HttpClientService();
        if ((TupleUtils.isTick(tuple)&&!MAILList.isEmpty())||MAILList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){            //定时
            Map<String, String> stringStringMap = httpClientService.gethttpFloodingPost(getFloodingPostUrl(), generate(schema, MAILList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_MAIL_TOPIC);
            logger.info("返回信息"+stringStringMap);
            maildatasize = maildatasize + getValidLength(generate(schema, MAILList));
            MAILList.clear();

                //向下发送指标数据
                basicOutputCollector.emit(new Values(mailcount,mailloadfail,maildatasize));
                maildatasize = 0L;
                mailcount = 0L;
                mailloadfail = 0L;

        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_HTTP_STREAMID)){
                String line =tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord mail_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(mail_rzzb, sessionRecordLog);
                    MAILList.add(mail_rzzb);
                    mailcount++;
                } catch (Exception e) {
                    logger.error("mail_rzzb数据封装处理异常"+e);
                    mailloadfail++;
                }
            }
        }


}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_MAILCOUNT_FIELD,StreamAggregateConfig.FLOODING_MAILLOADFAIL_FIELD,StreamAggregateConfig.FLOODING_MAILDATASIZE_FIELD));
    }
    private void setIP_RZZB(GenericRecord mail_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        mail_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        mail_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        mail_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_rzzb.put("PROTO_ID","1");  ///--转化表 还没弄
        mail_rzzb.put("DETAIL_ID","1"); //同上
        mail_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        mail_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //根据mail_bcc填充
        /* mail_rzzb.put("YL2",sessionRecordLog.getMail_bcc);*/
        /*mail_rzzb.put("YL4",sessionRecordLog.getMail_bcc);*/
        //mail特有
        mail_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        mail_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());

        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_rzzb.put("MAIL_RECEVER",getMailRecever(sessionRecordLog.getMail_to()));
        mail_rzzb.put("RECEVER_NUM",getNum(sessionRecordLog.getMail_to()));

        mail_rzzb.put("MAIL_CC",sessionRecordLog.getMail_cc());
        mail_rzzb.put("CC_NUM",getNum(sessionRecordLog.getMail_cc()));

        mail_rzzb.put("CONTENT",sessionRecordLog.getMail_subject());
        mail_rzzb.put("MAIL_RECEVER",sessionRecordLog.getMail_content());
        mail_rzzb.put("ACCESSORY",getAccessory(sessionRecordLog.getMail_attachment_name()));

        mail_rzzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_rzzb.put("FILE_CONTENT",sessionRecordLog.getMail_attachment_content());
        mail_rzzb.put("PWD",sessionRecordLog.getMail_passwd());
        mail_rzzb.put("FJFWS",getFjfws(sessionRecordLog.getMail_from()));
        //mail_rzzb.put("FJFWS_SZD",sessionRecordLog.getMail_content()); //  //境内境外未知，根据Location填充
        mail_rzzb.put("SJFWS",getFjfws(sessionRecordLog.getMail_to()));
        //mail_rzzb.put("SJFWS_SZD",sessionRecordLog.getMail_content());//境内境外未知，根据Location填充

        mail_rzzb.put("XYLX",sessionRecordLog.getMail_protocol_type());

 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
