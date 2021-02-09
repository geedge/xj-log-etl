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


import static cn.ac.iie.storm.utils.general.GKAssignment.setIP_GKRZ;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestIP {
    public static void main(String[] args) throws IOException {

        List<GenericRecord> DNSList = new ArrayList<GenericRecord>();
        String str = "{\"common_action\":16,\"common_address_type\":4,\"common_c2s_byte_num\":71,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":1,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"183.247.173.159\",\"common_client_location\":\"Other,Other,China\",\"common_client_port\":12122,\"common_con_duration_ms\":0,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611051108,\"common_establish_latency_ms\":0,\"common_external_ip\":\"183.247.173.159\",\"common_internal_ip\":\"117.146.215.242\",\"common_l4_protocol\":\"IPv4_UDP\",\"common_l7_protocol\":\"DNS\",\"common_log_id\":96768794494537728,\"common_policy_id\":285,\"common_processing_time\":1611051069,\"common_recv_time\":1611051108,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"DNS\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"117.146.215.242\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":53,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.229\",\"common_start_time\":1611051108,\"common_stream_dir\":1,\"common_stream_trace_id\":14364576005,\"common_sub_action\":\"redirect\",\"common_user_region\":\"{\\\"method\\\":\\\"redirect\\\",\\\"traffic_forward\\\":{\\\"pzlx\\\":402,\\\"xzid\\\":0,\\\"lhid\\\":416,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉恐,涉黑,涉邪教,测试,诈骗,涉赌,涉爆,涉黄,涉毒,shehuang\\\",\\\"pzid\\\":\\\"0402000000000001\\\",\\\"flid\\\":0,\\\"enable\\\":0,\\\"timestamp\\\":1611030696},\\\"resolution\\\":[{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"1.1.1.2\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.5\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.4\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.2\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.3\\\"}]},{\\\"qtype\\\":\\\"A\\\",\\\"answer\\\":[{\\\"ttl\\\":{\\\"max\\\":300,\\\"min\\\":300},\\\"atype\\\":\\\"A\\\",\\\"value\\\":\\\"192.168.1.1\\\"}]}],\\\"protocol\\\":\\\"DNS\\\"}\",\"dns_aa\":0,\"dns_ancount\":0,\"dns_arcount\":0,\"dns_message_id\":34004,\"dns_nscount\":0,\"dns_opcode\":0,\"dns_qclass\":1,\"dns_qdcount\":1,\"dns_qname\":\"m.baidu.com\",\"dns_qr\":0,\"dns_qtype\":1,\"dns_ra\":0,\"dns_rcode\":0,\"dns_rd\":1,\"dns_sub\":1,\"dns_tc\":0}";
        Schema ipSchema;

        ipSchema = new Schema.Parser().parse(StreamAggregateConfig.IP_GKRZ_SCHEMA);

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

        GenericRecord ip_gkzb = new GenericData.Record(ipSchema);
        setIP_GKRZ(ip_gkzb, sessionRecordLog);
        DNSList.add(ip_gkzb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(ipSchema, DNSList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_IP_TOPIC);
        System.out.println(stringStringMap);
    }
}
