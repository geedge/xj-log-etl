package gktest;

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


import static cn.ac.iie.storm.utils.general.GKAssignment.setMAIL_GKZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestMail {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":16,\"common_address_type\":4,\"common_c2s_byte_num\":705,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":3,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"39.144.201.71\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":37002,\"common_con_duration_ms\":106,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611114749,\"common_establish_latency_ms\":106,\"common_external_ip\":\"39.144.201.71\",\"common_internal_ip\":\"183.192.170.139\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"HTTPS\",\"common_log_id\":97836281738856460,\"common_policy_id\":300,\"common_processing_time\":1611114696,\"common_recv_time\":1611114749,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"SSL\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"183.192.170.139\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":443,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.226\",\"common_start_time\":1611114749,\"common_stream_dir\":1,\"common_stream_trace_id\":87977380205179,\"common_sub_action\":\"rst\",\"common_user_region\":\"{\\\"method\\\":\\\"rst\\\",\\\"traffic_forward\\\":{\\\"pzlx\\\":405,\\\"xzid\\\":0,\\\"lhid\\\":410,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉黑,涉爆\\\",\\\"pzid\\\":\\\"0405000000000002\\\",\\\"flid\\\":0,\\\"yylx\\\":\\\"01\\\",\\\"enable\\\":0,\\\"zsmc\\\":\\\"测试0120\\\",\\\"timestamp\\\":1611114742},\\\"protocol\\\":\\\"SSL\\\"}\",\"http_domain\":\"qq.com\",\"ssl_ja3_hash\":\"9b02ebd3a43b62d825e1ac605b621dc8\",\"mail_account\":\"report@lunzn.com\",\"mail_protocol_type\":\"SMTP\"}";
         List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
        Schema mailSchema;

        mailSchema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_GKZB_SCHEMA);

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

        GenericRecord mail_gkzb = new GenericData.Record(mailSchema);
        setMAIL_GKZB(mail_gkzb, sessionRecordLog);
        MAILList.add(mail_gkzb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(mailSchema, MAILList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_MAIL_TOPIC);
        System.out.println(stringStringMap);
    }
}
