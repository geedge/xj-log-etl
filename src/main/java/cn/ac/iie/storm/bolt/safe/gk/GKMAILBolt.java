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

import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.MailTrans.getFjfws;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class GKMAILBolt extends BaseBasicBolt {
  private final static Logger logger = Logger.getLogger(GKMAILBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> mailList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private Long mailcount = 0L;
    private Long mailloadfail = 0L;
    private long maildatasize = 0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_GKZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        if (TupleUtils.isTick(tuple)&&!mailList.isEmpty()){             //定时
            maildatasize = maildatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(mailcount,mailloadfail,maildatasize));

            mailList.clear();
            maildatasize = 0L;
            mailcount = 0L;
            mailloadfail = 0L;
            generate = null;
        }else if(mailList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){           //分批
            maildatasize = maildatasize + getValidLength(generate);
            mailList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_MAIL_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord dns_gkzb = new GenericData.Record(schema);
                    setMAIL_GKZB(dns_gkzb, sessionRecordLog);
                    mailList.add(dns_gkzb);
                    generate = generate(schema, mailList);
                    mailcount++;
                } catch (Exception e) {
                    logger.error("dns_gkzb数据封装处理异常"+e);
                    mailloadfail++;

                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.GK_MAILCOUNT_FIELD,StreamAggregateConfig.GK_MAILLOADFAIL_FIELD,StreamAggregateConfig.GK_MAILDATASIZE_FIELD));

    }
    private void setMAIL_GKZB(GenericRecord mail_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_gkzb.put("PZID",sessionRecordLog.getPzid());
        mail_gkzb.put("FLID",sessionRecordLog.getFlid());
        mail_gkzb.put("XZID",sessionRecordLog.getXzid());
        mail_gkzb.put("LHID",sessionRecordLog.getLhid());
        mail_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        mail_gkzb.put("GKLB",sessionRecordLog.getGklb());
        mail_gkzb.put("yl3",sessionRecordLog.getLabels());
        mail_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        mail_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        mail_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        mail_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        mail_gkzb.put("DETAIL_ID","1"); //同上
        mail_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        mail_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //根据mail_bcc填充
        /* mail_rzzb.put("yl2",sessionRecordLog.getMail_bcc);*/
        /*mail_rzzb.put("yl4",sessionRecordLog.getMail_bcc);*/

        //mail特有
        mail_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        mail_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_gkzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());

        mail_gkzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_gkzb.put("MAIL_RECEVER",getMailRecever(sessionRecordLog.getMail_to()));
        mail_gkzb.put("RECEVER_NUM",getNum(sessionRecordLog.getMail_to()));

        mail_gkzb.put("MAIL_CC",sessionRecordLog.getMail_cc());
        mail_gkzb.put("CC_NUM",getNum(sessionRecordLog.getMail_cc()));

        mail_gkzb.put("CONTENT",sessionRecordLog.getMail_subject());
        mail_gkzb.put("MAIL_RECEVER",sessionRecordLog.getMail_content());
        mail_gkzb.put("ACCESSORY",getAccessory(sessionRecordLog.getMail_attachment_name()));

        mail_gkzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_gkzb.put("FILE_CONTENT",sessionRecordLog.getMail_attachment_content());
        mail_gkzb.put("PWD",sessionRecordLog.getMail_passwd());
        mail_gkzb.put("FJFWS",getFjfws(sessionRecordLog.getMail_from()));
        //mail_rzzb.put("FJFWS_SZD",sessionRecordLog.getMail_content()); //  //境内境外未知，根据Location填充
        mail_gkzb.put("SJFWS",getFjfws(sessionRecordLog.getMail_to()));
        //mail_rzzb.put("SJFWS_SZD",sessionRecordLog.getMail_content());//境内境外未知，根据Location填充
        mail_gkzb.put("XYLX",sessionRecordLog.getMail_protocol_type());
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
