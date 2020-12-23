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

public class FloodingDNSBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(FloodingDNSBolt.class);
    private static final long serialVersionUID = -6795251425357896415L;
    private List<GenericRecord> IPList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private byte[] generate;
    private Long dnscount = 0L;
    private Long dnsloadfail = 0L;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        try {
            schema = new Schema.Parser().parse(new File("src/main/resources/floodingschema/DNS_RZZB.avsc"));

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!IPList.isEmpty()){             //定时
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            IPList.clear();

            basicOutputCollector.emit(new Values(dnscount,dnsloadfail));
            dnscount = 0L;
            dnsloadfail = 0L;
        }else if(IPList.size()>=StreamAggregateConfig.FLOODING_BATCH_INTER_NUM){           //分批
            httpClientService.gethttpFloodingPost("http://192.168.40.119:6699",generate,100);
            IPList.clear();
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
                dnscount++;
                try {
                    GenericRecord http_rzzb = new GenericData.Record(schema);
                    setDNS_RZZB(http_rzzb, sessionRecordLog);
                    IPList.add(http_rzzb);
                    System.out.println(http_rzzb);
                    generate = generate(schema, IPList);
                } catch (Exception e) {
                    e.printStackTrace();
                    dnsloadfail++;
                }
            }
        }

}
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("dnscount","dnsloadfail"));
    }
    private void setDNS_RZZB(GenericRecord dns_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_rzzb.put("YDY","ff");
        dns_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
       // IP_rzzb.put("QDJIP",sessionRecordLog.getCommon_sled_ip());
        dns_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_rzzb.put("SIP",111);
        dns_rzzb.put("DIP",sessionRecordLog.getCommon_server_ip());
        dns_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
       //字段类型不一致，还未处理
        dns_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        dns_rzzb.put("DETAIL_ID","1"); //同上
        dns_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_rzzb.put("YYS",1); //-**common_isp这个字段
        dns_rzzb.put("yl1",sessionRecordLog.getCommon_address_type());
     //dns特有
        dns_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        dns_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        dns_rzzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        dns_rzzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
 }
    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, StreamAggregateConfig.FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
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
