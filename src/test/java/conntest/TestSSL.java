package conntest;

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

import static cn.ac.iie.storm.utils.general.CONNAssignment.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestSSL {
    public static void main(String[] args) throws IOException {
        List<GenericRecord> SSLList = new ArrayList<GenericRecord>();
        Schema sslSchema;
        sslSchema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_RZZB_SCHEMA);
        String str = "{\"common_action\":0,\"common_address_type\":4,\"common_c2s_byte_num\":0,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"223.115.228.160\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":6450,\"common_con_duration_ms\":30957,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":69,\"common_end_time\":1611815268,\"common_establish_latency_ms\":83,\"common_external_ip\":\"162.14.14.11\",\"common_internal_ip\":\"223.115.228.160\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"HTTP\",\"common_log_id\":109589882311743491,\"common_policy_id\":0,\"common_processing_time\":1611815266,\"common_protocol_label\":\"TCP/IPv4/ETHERNET\",\"common_recv_time\":1611815268,\"common_s2c_byte_num\":1516,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":4,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"HTTP\",\"common_server_asn\":\"132203\",\"common_server_ip\":\"162.14.14.11\",\"common_server_location\":\"Other,Other,Hong Kong\",\"common_server_port\":80,\"common_sessions\":1,\"common_sled_ip\":\"10.111.128.239\",\"common_start_time\":1611815268,\"common_stream_dir\":2,\"common_stream_trace_id\":123184706918968,\"http_content_length\":\"1134\",\"http_content_type\":\"application/json\",\"http_proxy_flag\":0,\"http_sequence\":1,\"http_version\":\"http1\"}";
       SessionRecordLog sessionRecordLog = JSONObject.parseObject(str, SessionRecordLog.class);
        GenericRecord jstx_rzzb = new GenericData.Record(sslSchema);

        setHTTP_RZZB(jstx_rzzb, sessionRecordLog);
        SSLList.add(jstx_rzzb);
        HttpClientService httpClientService = new HttpClientService();

        Map<String, String> stringStringMap = httpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(sslSchema, SSLList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_SSL_TOPIC);
        System.out.println(stringStringMap);
        SSLList.clear();

    }

}
