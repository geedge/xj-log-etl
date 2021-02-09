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

import static cn.ac.iie.storm.utils.general.JCAssignment.setFTP_JCZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestFTP {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":1,\"common_address_type\":4,\"common_c2s_byte_num\":2884,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":13,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"223.116.148.253\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":9410,\"common_con_duration_ms\":34024,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611224156,\"common_establish_latency_ms\":243,\"common_external_ip\":\"223.116.148.253\",\"common_internal_ip\":\"117.144.244.26\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"HTTPS\",\"common_log_id\":99672745028108302,\"common_policy_id\":341,\"common_processing_time\":1611224158,\"common_recv_time\":1611224156,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"SSL\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"117.144.244.26\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":443,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.251\",\"common_start_time\":1611224152,\"common_stream_dir\":1,\"common_stream_trace_id\":20034944269,\"common_user_region\":\"{\\\"traffic_forward\\\":{\\\"pzlx\\\":505,\\\"xzid\\\":0,\\\"lhid\\\":410,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉爆,涉黄\\\",\\\"pzid\\\":\\\"0505000000000004\\\",\\\"flid\\\":0,\\\"yylx\\\":\\\"00\\\",\\\"enable\\\":0,\\\"zsmc\\\":\\\"qq.com\\\",\\\"timestamp\\\":1611222773},\\\"protocol\\\":\\\"SSL\\\"}\",\"http_domain\":\"qq.com\",\"ssl_ja3_hash\":\"0b87118411d0a3fb5002ce9ad0acbaa9\",\"ssl_sni\":\"lbs.map.qq.com\"}";
        List<GenericRecord> FTPList = new ArrayList<GenericRecord>();
        Schema ftpSchema;

        ftpSchema = new Schema.Parser().parse(StreamAggregateConfig.FTP_JCZB_SCHEMA);

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

        GenericRecord ftp_jczb = new GenericData.Record(ftpSchema);
        setFTP_JCZB(ftp_jczb, sessionRecordLog);
        FTPList.add(ftp_jczb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(ftpSchema, FTPList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_FTP_TOPIC);
        System.out.println(stringStringMap);
    }
}
