package jctest;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.bean.Traffic_Forward;
import cn.ac.iie.storm.bean.UserRegion;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.GKAssignment.setWYGJC_GKZB;
import static cn.ac.iie.storm.utils.general.JCAssignment.setWYGJC_JCZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestWygjc {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":16,\"common_address_type\":4,\"common_c2s_byte_num\":71,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":1,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"183.247.173.159\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":12122,\"common_con_duration_ms\":0,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611051108,\"common_establish_latency_ms\":0,\"common_external_ip\":\"183.247.173.159\",\"common_internal_ip\":\"117.146.215.242\",\"common_l4_protocol\":\"IPv4_UDP\",\"common_l7_protocol\":\"DNS\",\"common_log_id\":96768794494537728,\"common_policy_id\":285,\"common_processing_time\":1611051069,\"common_recv_time\":1611051108,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"DNS\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"117.146.215.242\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":53,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.229\",\"common_start_time\":1611051108,\"common_stream_dir\":1,\"common_stream_trace_id\":14364576005,\"common_sub_action\":\"redirect\",\"common_user_region\":\"{\\\"method\\\":\\\"redirect\\\",\\\"traffic_forward\\\":{\\\"pzlx\\\":409,\\\"xzid\\\":0,\\\"lhid\\\":416,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉恐,涉黑,涉邪教,测试,诈骗,涉赌,涉爆,涉黄,涉毒,shehuang\\\",\\\"pzid\\\":\\\"0402000000000001\\\",\\\"flid\\\":0,\\\"enable\\\":0,\\\"timestamp\\\":1611030696},\\\"resolution\\\":[{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"1.1.1.2\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.5\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.4\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.2\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.3\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.1\\\"}]}],\\\"protocol\\\":\\\"DNS\\\"}\"}";
        List<GenericRecord> WYGJCList = new ArrayList<GenericRecord>();
        Schema wygjcSchema;

        wygjcSchema = new Schema.Parser().parse(StreamAggregateConfig.WYGJZ_JCZB_SCHEMA);

        SessionRecordLog sessionRecordLog = JSONObject.parseObject(str, SessionRecordLog.class);
        UserRegion userRegion = JSONObject.parseObject(sessionRecordLog.getCommon_user_region(), UserRegion.class);
        Traffic_Forward traffic_forward = JSONObject.parseObject(userRegion.getTraffic_forward(), Traffic_Forward.class);
        sessionRecordLog.setPzid(traffic_forward.getPzid());
        sessionRecordLog.setFlid(traffic_forward.getFlid());
        sessionRecordLog.setXzid(traffic_forward.getXzid());
        sessionRecordLog.setLhid(traffic_forward.getLhid());
        sessionRecordLog.setPzlx(traffic_forward.getPzlx());
        sessionRecordLog.setGklb(traffic_forward.getGklb());
        sessionRecordLog.setLabels(traffic_forward.getLabels());
        sessionRecordLog.setYyzl(traffic_forward.getYyzl());

        GenericRecord wygjc_jczb = new GenericData.Record(wygjcSchema);
        setWYGJC_JCZB(wygjc_jczb, sessionRecordLog);
        WYGJCList.add(wygjc_jczb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(wygjcSchema, WYGJCList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_WYGJC_TOPIC);
        System.out.println(stringStringMap);
    }
}
