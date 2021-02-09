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

import static cn.ac.iie.storm.utils.general.JCAssignment.setHTTP_JCZB;
import static cn.ac.iie.storm.utils.general.JCAssignment.setIP_JCRZ;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class JCIp {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":1,\"common_address_type\":4,\"common_c2s_byte_num\":2062,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":6,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"223.117.24.8\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":16089,\"common_con_duration_ms\":236,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611223971,\"common_establish_latency_ms\":134,\"common_external_ip\":\"223.117.24.8\",\"common_internal_ip\":\"183.194.238.117\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"HTTP\",\"common_log_id\":99671189075544078,\"common_policy_id\":342,\"common_processing_time\":1611224065,\"common_recv_time\":1611223971,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"HTTP\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"183.194.238.117\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":80,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.246\",\"common_start_time\":1611223971,\"common_stream_dir\":1,\"common_stream_trace_id\":105573145241355,\"common_user_region\":\"{\\\"traffic_forward\\\":{\\\"pzlx\\\":504,\\\"xzid\\\":0,\\\"lhid\\\":410,\\\"yyzl\\\":0,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉黑,涉邪教\\\",\\\"pzid\\\":\\\"0504000000000002\\\",\\\"flid\\\":0,\\\"yylx\\\":\\\"00\\\",\\\"enable\\\":0,\\\"timestamp\\\":1611222786},\\\"protocol\\\":\\\"HTTP\\\"}\",\"http_content_length\":\"1420\",\"http_content_type\":\"multipart/form-data; boundary=-----------hello word-----------\",\"http_domain\":\"qq.com\",\"http_host\":\"www.qq.com\",\"http_proxy_flag\":0,\"http_response_lantency_ms\":0,\"http_sequence\":1,\"http_session_duration_ms\":101,\"http_url\":\"www.qq.com/\",\"http_user_agent\":\"Dalvik/2.1.0 (Linux; U; Android 10; PCGM00 Build/QKQ1.190918.001)\",\"http_version\":\"http1\"}";
        List<GenericRecord> IPList = new ArrayList<GenericRecord>();
        Schema ipSchema;

        ipSchema = new Schema.Parser().parse(StreamAggregateConfig.IP_JCZB_SCHEMA);

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

        GenericRecord ip_jczb = new GenericData.Record(ipSchema);
        setIP_JCRZ(ip_jczb, sessionRecordLog);
        IPList.add(ip_jczb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(ipSchema, IPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_IP_TOPIC);
        System.out.println(stringStringMap);
    }
}
