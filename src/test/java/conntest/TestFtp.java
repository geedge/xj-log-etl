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

import static cn.ac.iie.storm.utils.general.CONNAssignment.setFTP_RZZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestFtp {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_schema_type\":\"FTP\",\"common_sessions\":1,\"http_response_line\":\"HTTP/1.0 200 OK\",\"http_content_type\":\"application/octet-stream\",\"http_content_length\":\"935\",\"http_isn\":2030451978,\"http_proxy_flag\":0,\"http_version\":\"http1\",\"common_protocol_label\":\"TCP/IPv4/ETHERNET\",\"http_sequence\":1,\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_c2s_tcp_unorder_num\":0,\"common_s2c_tcp_unorder_num\":0,\"common_c2s_tcp_lostlen\":0,\"common_s2c_tcp_lostlen\":0,\"common_server_ip\":\"117.190.80.44\",\"common_client_ip\":\"120.230.167.94\",\"common_server_port\":80,\"common_client_port\":17455,\"common_stream_dir\":2,\"common_address_type\":4,\"common_s2c_pkt_num\":5,\"common_s2c_byte_num\":1334,\"common_c2s_pkt_num\":0,\"common_c2s_byte_num\":0,\"common_start_time\":1611030829,\"common_end_time\":1611030830,\"common_con_duration_ms\":31455,\"common_stream_trace_id\":\"87974630312906\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_address_list\":\"17455-80-120.230.167.94-117.190.80.44\",\"common_l7_protocol\":\"HTTP\",\"common_establish_latency_ms\":595,\"common_client_location\":\"Jiangmen,Guangdong,China\",\"common_server_location\":\"Other,Other,China\",\"common_direction\":69,\"common_sled_ip\":\"10.111.128.219\",\"common_device_id\":\"GN202000000000000000\",\"common_data_center\":\"CMCC\",\"common_policy_id\":0,\"common_service\":2,\"common_action\":0}";
        List<GenericRecord> FTPList = new ArrayList<GenericRecord>();
        Schema ftpSchema;

        ftpSchema = new Schema.Parser().parse(StreamAggregateConfig.FTP_RZZB_SCHEMA);
        SessionRecordLog sessionRecordLog = JSONObject.parseObject(str, SessionRecordLog.class);

        GenericRecord ftp_rzzb = new GenericData.Record(ftpSchema);
        setFTP_RZZB(ftp_rzzb, sessionRecordLog);
        FTPList.add(ftp_rzzb);
        System.out.println(ftp_rzzb.get("PROTO_ID"));

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(ftpSchema, FTPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_FTP_TOPIC);
        System.out.println(stringStringMap);
    }
}
