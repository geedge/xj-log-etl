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

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class GKVPNBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GKVPNBolt.class);
    private static final long serialVersionUID = 4909453288066034335L;
    private List<GenericRecord> vpnList = new ArrayList<GenericRecord>();
    private Schema schema;
    private SessionRecordLog sessionRecordLog;
    private long vpncount = 0L;
    private long vpnloadfail = 0L;
    private long vpndatasize = 0L;
    private byte[] generate=null;


    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        schema = new Schema.Parser().parse(StreamAggregateConfig.VPN_GKZB_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        HttpClientService httpClientService = new HttpClientService();
        if (TupleUtils.isTick(tuple)&&!vpnList.isEmpty()){  //定时
           // httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate,StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            vpndatasize  =  vpndatasize + getValidLength(generate);

            basicOutputCollector.emit(new Values(vpncount,vpnloadfail,vpndatasize));

            vpnList.clear();
            vpndatasize = 0L;
            vpncount = 0L;
            vpnloadfail = 0L;
            generate = null;
        }else if(vpnList.size()>=StreamAggregateConfig.SAFE_BATCH_INTER_NUM){       //分批
          //  httpClientService.gethttpFloodingPost(StreamAggregateConfig.FLOODING_HTTP_POST_URL,generate(schema, vpnList),StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT);
            vpndatasize  =  vpndatasize + getValidLength(generate);
            vpnList.clear();
            generate = null;
        }
        else {
            if(tuple.getSourceStreamId().equals(StreamAggregateConfig.GK_VPN_STREAMID)){
                sessionRecordLog =(SessionRecordLog)tuple.getValue(0);
            }
            if (sessionRecordLog != null) {
                try {
                    GenericRecord vpn_rzzb = new GenericData.Record(schema);
                    setVPN_GKZB(vpn_rzzb, sessionRecordLog);
                    vpnList.add(vpn_rzzb);
                    generate = generate(schema, vpnList);
                    vpncount++;
                } catch (Exception e) {
                    logger.error("vpn_gkzb数据封装处理异常"+e);
                    vpnloadfail++;
                }
            }
        }
}

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
            outputFieldsDeclarer.declare(new Fields(StreamAggregateConfig.GK_VPNCOUNT_FIELD,StreamAggregateConfig.GK_VPNLOADFAIL_FIELD,StreamAggregateConfig.GK_VPNDATASIZE_FIELD));
    }
    private void setVPN_GKZB(GenericRecord vpn_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        vpn_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_gkzb.put("PZID",sessionRecordLog.getPzid());
        vpn_gkzb.put("FLID",sessionRecordLog.getFlid());
        vpn_gkzb.put("XZID",sessionRecordLog.getXzid());
        vpn_gkzb.put("LHID",sessionRecordLog.getLhid());
        vpn_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        vpn_gkzb.put("GKLB",sessionRecordLog.getGklb());
        vpn_gkzb.put("yl3",sessionRecordLog.getLabels());
        vpn_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        vpn_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        vpn_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        vpn_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        vpn_gkzb.put("DETAIL_ID","1"); //同上
        vpn_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        vpn_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        vpn_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //vpn特有
        vpn_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        vpn_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        vpn_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        vpn_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
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
