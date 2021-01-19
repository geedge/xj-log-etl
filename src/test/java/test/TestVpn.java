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

import static cn.ac.iie.storm.utils.general.Assignment.setVPN_RZZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;


public class TestVpn {
    public static void main(String[] args) throws IOException {
        List<GenericRecord> VPNList = new ArrayList<GenericRecord>();
        Schema vpnSchema;
        String str = "{\"common_schema_type\":\"BASE\",\"common_sessions\":1,\"common_protocol_label\":\"UDP/IPv4/ETHERNET\",\"common_c2s_ipfrag_num\":0,\"common_s2c_ipfrag_num\":0,\"common_server_ip\":\"223.115.12.126\",\"common_client_ip\":\"111.21.91.210\",\"common_server_port\":13823,\"common_client_port\":18005,\"common_stream_dir\":2,\"common_address_type\":4,\"common_s2c_pkt_num\":33,\"common_s2c_byte_num\":10049,\"common_c2s_pkt_num\":0,\"common_c2s_byte_num\":0,\"common_start_time\":1611028993,\"common_end_time\":1611028998,\"common_con_duration_ms\":65324,\"common_stream_trace_id\":\"43994105029885\",\"common_l4_protocol\":\"IPv4_UDP\",\"common_address_list\":\"18005-13823-111.21.91.210-223.115.12.126\",\"common_l7_protocol\":\"PPTP\",\"common_establish_latency_ms\":0,\"common_client_location\":\"Xi'an,Shaanxi,China\",\"common_server_location\":\"Other,Other,China\",\"common_direction\":69,\"common_sled_ip\":\"10.111.128.229\",\"common_device_id\":\"GN202000000000000000\",\"common_data_center\":\"CMCC\",\"common_policy_id\":0,\"common_service\":2,\"common_action\":0}";
        vpnSchema = new Schema.Parser().parse(StreamAggregateConfig.VPN_RZZB_SCHEMA);
        SessionRecordLog sessionRecordLog = JSONObject.parseObject(str, SessionRecordLog.class);

        if (sessionRecordLog.getCommon_l7_protocol().equals("PPTP") || sessionRecordLog.getCommon_l7_protocol().equals("L2TP") || sessionRecordLog.getCommon_l7_protocol().equals("OPENVPN") || sessionRecordLog.getCommon_l7_protocol().equals("IPSEC")) {
            GenericRecord vpn_rzzb = new GenericData.Record(vpnSchema);
            setVPN_RZZB(vpn_rzzb, sessionRecordLog);
            VPNList.add(vpn_rzzb);
            HttpClientService httpClientService = new HttpClientService();
            System.out.println(vpn_rzzb.get("PROTO_ID"));

            Map<String, String> stringStringMap = httpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(vpnSchema, VPNList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.FLOODING_POST_VPN_TOPIC);

            System.out.println(stringStringMap);

        }
    }
}
