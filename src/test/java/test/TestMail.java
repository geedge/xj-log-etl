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

import static cn.ac.iie.storm.utils.general.Assignment.setMAIL_RZZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestMail {
    public static void main(String[] args) throws IOException {
       List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
        Schema mailSchema;
        mailSchema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_RZZB_SCHEMA);

        String jsonStr = "{\"common_action\":0,\"common_address_type\":4,\"common_c2s_byte_num\":606,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":9,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"36.184.3.197\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":5100,\"common_con_duration_ms\":31462,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611024036,\"common_establish_latency_ms\":269,\"common_external_ip\":\"36.184.3.197\",\"common_internal_ip\":\"183.232.93.197\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"UNKNOWN_TCP\",\"common_log_id\":96315054955890691,\"common_policy_id\":0,\"common_processing_time\":1611024024,\"common_protocol_label\":\"TCP/IPv4/ETHERNET\",\"common_recv_time\":1611024036,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"MAIL\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"183.232.93.197\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":25,\"common_sessions\":1,\"common_sled_ip\":\"10.111.128.240\",\"common_start_time\":1611024035,\"common_stream_dir\":1,\"common_stream_trace_id\":61586128607366,\"mail_account\":\"report@lunzn.com\",\"mail_protocol_type\":\"SMTP\"}";
        SessionRecordLog sessionRecordLog = JSONObject.parseObject(jsonStr, SessionRecordLog.class);
        GenericRecord mail_rzzb = new GenericData.Record(mailSchema);
        try {
            setMAIL_RZZB(mail_rzzb, sessionRecordLog);
        } catch (IOException e) {
            e.printStackTrace();
        }
        MAILList.add(mail_rzzb);
        HttpClientService httpClientService = new HttpClientService();

        Map<String, String> stringStringMap = httpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(mailSchema, MAILList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_MAIL_TOPIC);
        System.out.println(stringStringMap);
        MAILList.clear();
    }
}
