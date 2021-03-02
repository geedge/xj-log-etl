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

import static cn.ac.iie.storm.utils.general.JCAssignment.setDNS_JCZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestDns {
    public static void main(String[] args) throws IOException {
       String str = "{\"common_action\":1,\"common_address_type\":4,\"common_c2s_byte_num\":85,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":1,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"4134\",\"common_client_ip\":\"49.114.170.81\",\"common_client_location\":\",,\",\"common_client_port\":20991,\"common_con_duration_ms\":1,\"common_data_center\":\"CTCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1614496480,\"common_establish_latency_ms\":0,\"common_external_ip\":\"49.114.170.81\",\"common_internal_ip\":\"114.114.114.114\",\"common_l4_protocol\":\"IPv4_UDP\",\"common_l7_protocol\":\"DNS\",\"common_log_id\":154573189755715584,\"common_policy_id\":1351,\"common_processing_time\":1614496480,\"common_recv_time\":1614496480,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"DNS\",\"common_server_asn\":\"174,4134\",\"common_server_ip\":\"114.114.114.114\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":53,\"common_sessions\":0,\"common_sled_ip\":\"10.111.64.11\",\"common_start_time\":1614496480,\"common_stream_dir\":1,\"common_stream_trace_id\":127262545176,\"common_user_region\":\"{\\\"traffic_forward\\\":{\\\"pzlx\\\":502,\\\"xzid\\\":0,\\\"lhid\\\":333,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉黑,涉邪教,诈骗,涉爆,涉黄,涉毒,测试\\\",\\\"pzid\\\":\\\"0502000000000007\\\",\\\"flid\\\":0,\\\"enable\\\":0,\\\"timestamp\\\":1611985457},\\\"protocol\\\":\\\"DNS\\\"}\",\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":57215,\"dns_nscount\":0,\"dns_opcode\":0,\"dns_qclass\":1,\"dns_qdcount\":1,\"dns_qname\":\"pull-flv-f6.douyincdn.com\",\"dns_qr\":0,\"dns_qtype\":1,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":1,\"dns_sub\":1,\"dns_tc\":0}";
       List<GenericRecord> DNSList = new ArrayList<GenericRecord>();
        Schema dnsSchema;

        dnsSchema = new Schema.Parser().parse(StreamAggregateConfig.DNS_JCZB_SCHEMA);

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

        GenericRecord dns_jczb = new GenericData.Record(dnsSchema);
        setDNS_JCZB(dns_jczb, sessionRecordLog);
        DNSList.add(dns_jczb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(dnsSchema, DNSList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_DNS_TOPIC);
        System.out.println(stringStringMap);
    }
}
