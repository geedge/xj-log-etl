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
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class FloodingMAILBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingMAILBolt.class);
    private static final long serialVersionUID = 4909453288066034335L;
    private List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
    private Schema schema;
    private Long mailcount;
    private Long mailloadfail;
    private SessionRecordLog sessionRecordLog;
    private byte[] generate;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            schema = new Schema.Parser()
                    .parse(Thread.currentThread().getContextClassLoader().getResourceAsStream("floodingschema/IP_RZZB.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!MAILList.isEmpty()){     //定时
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            MAILList.clear();

            basicOutputCollector.emit(new Values(mailcount,mailloadfail));
            mailcount = 0L;
            mailloadfail = 0L;
        }else if(MAILList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){      //分批
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            MAILList.clear();
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.FLOODING_HTTP_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                mailcount++;
                try {
                    GenericRecord http_rzzb = new GenericData.Record(schema);
                    setIP_RZZB(http_rzzb, sessionRecordLog);
                    MAILList.add(http_rzzb);
                    System.out.println(http_rzzb);
                    generate = generate(schema, MAILList);
                } catch (Exception e) {
                    e.printStackTrace();
                    mailloadfail++;
                }
            }
        }


}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.FLOODING_MAILCOUNT_FIELD,StreamAggregateConfig.FLOODING_MAILLOADFAIL_FIELD));
    }
    private void setIP_RZZB(GenericRecord mail_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_rzzb.put("YDY","ff");
        mail_rzzb.put("MDDY","暂时没有");
        mail_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        mail_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));
        //字段类型不一致，还未处理
        mail_rzzb.put("DETAIL_ID","1"); //还没处理
        mail_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_rzzb.put("YYS",1); //-**common_isp这个字段
        mail_rzzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //根据mail_bcc填充
       /* mail_rzzb.put("yl2",sessionRecordLog.getMail_bcc);*/
        /*mail_rzzb.put("yl4",sessionRecordLog.getMail_bcc);*/

     //mail特有
        mail_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        mail_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        //还未处理  recever_num
        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_rzzb.put("MAIL_RECEVER",sessionRecordLog.getMail_to());

        //根据mail_to填充
        /*mail_rzzb.put("RECEVER_NUM",sessionRecordLog.getMail_to());*/


        mail_rzzb.put("MAIL_CC",sessionRecordLog.getMail_cc());

       // 根据mail_cc填
        /*mail_rzzb.put("CC_NUM",sessionRecordLog.getMail_to());*/

        mail_rzzb.put("CONTENT",sessionRecordLog.getMail_subject());
        mail_rzzb.put("MAIL_RECEVER",sessionRecordLog.getMail_content());

        //根据是否存在mail_attachment_name填充
        /*mail_rzzb.put("ACCESSORY",sessionRecordLog.getMail_attachment_name());*/

        mail_rzzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_rzzb.put("FILE_CONTENT",sessionRecordLog.getMail_attachment_content());
        mail_rzzb.put("PWD",sessionRecordLog.getMail_passwd());

        //根据mail_from填充
        /*mail_rzzb.put("FJFWS",sessionRecordLog.getMail_content());*/
        //境内境外未知，根据Location填充
         /*mail_rzzb.put("FJFWS_SZD",sessionRecordLog.getMail_content());*/
        //根据mail_to填充
          /*mail_rzzb.put("SJFWS",sessionRecordLog.getMail_content());*/
        //境内境外未知，根据Location填充
        /*mail_rzzb.put("SJFWS_SZD",sessionRecordLog.getMail_content());*/

        //mail_protocol_type
        /*mail_rzzb.put("XYLX",sessionRecordLog.getMail_protocol_type);*/

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
