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


import static cn.ac.iie.storm.utils.general.GKAssignment.setVPN_GKZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestVpn {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":16,\"common_address_type\":4,\"common_c2s_byte_num\":68964,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":75,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"223.117.129.189\",\"common_client_location\":\"Yining,Xinjiang,China\",\"common_client_port\":26752,\"common_con_duration_ms\":38737,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":73,\"common_end_time\":1611116271,\"common_establish_latency_ms\":96,\"common_external_ip\":\"223.117.129.189\",\"common_internal_ip\":\"120.241.186.100\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"PPTP\",\"common_log_id\":97862110380326925,\"common_policy_id\":304,\"common_processing_time\":1611116236,\"common_recv_time\":1611116271,\"common_s2c_byte_num\":0,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":0,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"HTTP\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"120.241.186.100\",\"common_server_location\":\"Other,Other,China\",\"common_server_port\":80,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.223\",\"common_start_time\":1611116233,\"common_stream_dir\":1,\"common_stream_trace_id\":17608684656699,\"common_sub_action\":\"rst\",\"common_user_region\":\"{\\\"method\\\":\\\"rst\\\",\\\"traffic_forward\\\":{\\\"pzlx\\\":411,\\\"xzid\\\":0,\\\"lhid\\\":410,\\\"yyzl\\\":0,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉黑\\\",\\\"pzid\\\":\\\"0403000000000005\\\",\\\"flid\\\":0,\\\"yylx\\\":\\\"00\\\",\\\"enable\\\":0,\\\"timestamp\\\":1611116084},\\\"protocol\\\":\\\"HTTP\\\"}\",\"http_domain\":\"qq.com\",\"http_host\":\"btrace.qq.com\",\"http_proxy_flag\":0,\"http_sequence\":60,\"http_url\":\"btrace.qq.com/ckvcollect/\",\"http_version\":\"http1\"}";
        List<GenericRecord> VPNList = new ArrayList<GenericRecord>();
        Schema vpnSchema;

        vpnSchema = new Schema.Parser().parse(StreamAggregateConfig.VPN_GKZB_SCHEMA);

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

        GenericRecord vpn_gkzb = new GenericData.Record(vpnSchema);
        setVPN_GKZB(vpn_gkzb, sessionRecordLog);
        VPNList.add(vpn_gkzb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(vpnSchema, VPNList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.GK_POST_VPN_TOPIC);
        System.out.println(stringStringMap);
    }
}
