package test;

import cn.ac.iie.storm.bean.SessionRecordLog;
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

import static cn.ac.iie.storm.utils.general.Assignment.setJSTX_RZZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestJstx {
    public static void main(String[] args) throws IOException {
        List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();
        Schema jstxSchema;
        jstxSchema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_RZZB_SCHEMA);

        String jsonStr = "{\"common_schema_type\":\"APP\",\"common_sessions\":1,\"http_request_line\":\"POST /wdinfo.php HTTP/1.1\",\"http_content_type\":\"application/octet-stream\",\"http_user_agent\":\"Post\",\"http_host\":\"qurl.f.360.cn\",\"http_url\":\"qurl.f.360.cn/wdinfo.php\",\"http_content_length\":\"894\",\"http_isn\":4280345344,\"http_proxy_flag\":0,\"http_version\":\"http1\",\"common_protocol_label\":\"TCP/IPv4/ETHERNET\",\"http_sequence\":1,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_server_ip\":\"111.7.68.189\",\"common_client_ip\":\"112.43.6.253\",\"common_server_port\":80,\"common_client_port\":9519,\"common_stream_dir\":1,\"common_address_type\":4,\"common_s2c_pkt_num\":0,\"common_s2c_byte_num\":0,\"common_c2s_pkt_num\":6,\"common_c2s_byte_num\":1402,\"common_start_time\":1611031676,\"common_end_time\":1611031676,\"common_con_duration_ms\":31020,\"common_stream_trace_id\":\"105566844157394\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_address_list\":\"9519-80-112.43.6.253-111.7.68.189\",\"common_l7_protocol\":\"HTTP\",\"common_establish_latency_ms\":54,\"common_client_location\":\"Other,Other,China\",\"common_server_location\":\"Other,Other,China\",\"common_direction\":73,\"common_sled_ip\":\"10.111.128.248\",\"common_device_id\":\"GN202000000000000000\",\"common_data_center\":\"CMCC\",\"common_policy_id\":0,\"common_service\":2,\"common_action\":0,\"common_app_id\":\"GO短信\"}";
        SessionRecordLog sessionRecordLog = JSONObject.parseObject(jsonStr, SessionRecordLog.class);
        GenericRecord jstx_rzzb = new GenericData.Record(jstxSchema);
        setJSTX_RZZB(jstx_rzzb, sessionRecordLog);
        JSTXList.add(jstx_rzzb);
        HttpClientService httpClientService = new HttpClientService();

        Map<String, String> stringStringMap = httpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(jstxSchema, JSTXList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_JSTX_TOPIC);
        System.out.println(stringStringMap);
        JSTXList.clear();
    }
}
