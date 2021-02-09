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

import static cn.ac.iie.storm.utils.general.JCAssignment.setHTTP_JCZB;
import static cn.ac.iie.storm.utils.general.JCAssignment.setSSL_JCZB;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class JcSsl {
    public static void main(String[] args) throws IOException {
        String str = "{\"common_action\":1,\"common_address_type\":4,\"common_c2s_byte_num\":2396,\"common_c2s_ipfrag_num\":0,\"common_c2s_pkt_num\":17,\"common_c2s_tcp_lostlen\":0,\"common_c2s_tcp_unorder_num\":0,\"common_client_asn\":\"9808\",\"common_client_ip\":\"223.114.176.221\",\"common_client_location\":\"650000\",\"common_client_port\":2245,\"common_con_duration_ms\":8508,\"common_data_center\":\"CMCC\",\"common_device_id\":\"GN202000000000000000\",\"common_direction\":69,\"common_end_time\":1611889277,\"common_establish_latency_ms\":97,\"common_external_ip\":\"183.232.232.23\",\"common_internal_ip\":\"223.114.176.221\",\"common_l4_protocol\":\"IPv4_TCP\",\"common_l7_protocol\":\"HTTPS\",\"common_log_id\":110834274226219014,\"common_policy_id\":443,\"common_processing_time\":1611889437,\"common_recv_time\":1611889277,\"common_s2c_byte_num\":5512,\"common_s2c_ipfrag_num\":0,\"common_s2c_pkt_num\":18,\"common_s2c_tcp_lostlen\":0,\"common_s2c_tcp_unorder_num\":0,\"common_schema_type\":\"SSL\",\"common_server_asn\":\"9808\",\"common_server_ip\":\"183.232.232.23\",\"common_server_location\":\"640000\",\"common_server_port\":443,\"common_sessions\":0,\"common_sled_ip\":\"10.111.128.252\",\"common_start_time\":1611889269,\"common_stream_dir\":3,\"common_stream_trace_id\":88002759659387,\"common_user_region\":\"{\\\"traffic_forward\\\":{\\\"pzlx\\\":505,\\\"xzid\\\":0,\\\"lhid\\\":410,\\\"gklb\\\":4,\\\"labels\\\":\\\"涉邪教,涉爆,涉毒,测试\\\",\\\"pzid\\\":\\\"0505000000000001\\\",\\\"flid\\\":0,\\\"yylx\\\":\\\"01\\\",\\\"enable\\\":0,\\\"zsmc\\\":\\\"0119ceshi\\\",\\\"timestamp\\\":1611043923},\\\"protocol\\\":\\\"SSL\\\"}\",\"http_domain\":\"baidu.com\",\"ssl_cn\":\"baidu.com\",\"ssl_ja3_hash\":\"e46f2a8067ae28e46fa732fcb3a4f2e8\",\"ssl_san\":\"baidu.com;click.hm.baidu.com;cm.pos.baidu.com;log.hm.baidu.com;update.pan.baidu.com;wn.pos.baidu.com;*.91.com;*.aipage.cn;*.aipage.com;*.apollo.auto;*.baidu.com;*.baidubce.com;*.baiducontent.com;*.baidupcs.com;*.baidustatic.com;*.baifubao.com;*.bce.baidu.com;*.bcehost.com;*.bdimg.com;*.bdstatic.com;*.bdtjrcv.com;*.bj.baidubce.com;*.chuanke.com;*.cloud.baidu.com;*.dlnel.com;*.dlnel.org;*.dueros.baidu.com;*.eyun.baidu.com;*.fanyi.baidu.com;*.gz.baidubce.com;*.hao123.baidu.com;*.hao123.com;*.hao222.com;*.haokan.com;*.im.baidu.com;*.map.baidu.com;*.mbd.baidu.com;*.mipcdn.com;*.news.baidu.com;*.nuomi.com;*.pae.baidu.com;*.safe.baidu.com;*.smartapps.cn;*.su.baidu.com;*.trustgo.com;*.vd.bdstatic.com;*.xueshu.baidu.com;apollo.auto;baifubao.com;dwz.cn;mct.y.nuomi.com;www.baidu.cn;www.baidu.com.cn\",\"ssl_sni\":\"pimlog.baidu.com\"}";
        List<GenericRecord> SSLList = new ArrayList<GenericRecord>();
        Schema sslSchema;

        sslSchema = new Schema.Parser().parse(StreamAggregateConfig.SSL_JCZB_SCHEMA);

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

        GenericRecord ssl_jczb = new GenericData.Record(sslSchema);
        setSSL_JCZB(ssl_jczb, sessionRecordLog);
        SSLList.add(ssl_jczb);

        Map<String, String> stringStringMap = HttpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(sslSchema, SSLList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_SSL_TOPIC);
        System.out.println(stringStringMap);
    }
}
