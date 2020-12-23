package cn.ac.iie.storm.utils.combine.flooding;

import cn.ac.iie.storm.bean.SessionRecordLog;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

/**
 * 对类型进行封装
 */
public class FloodingProcess_RZ {
    /**
     * 对DNS进行封装
     *
     * @param sessionRecordLog
     * @param sessionRecordLog
     * @return
     */
    public static void setDNS_RZZB(GenericRecord dns_rzzb, SessionRecordLog sessionRecordLog) throws IOException {


        dns_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        dns_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        //暂定强转
        //dns_rzzb.put("QDJIP",sessionRecordLog.getCommon_sled_ip());
        dns_rzzb.put("SIP",sessionRecordLog.getCommon_client_ip());
        dns_rzzb.put("DIP",sessionRecordLog.getCommon_server_ip());

        dns_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        //字段类型不一致，还未处理
        //dns_rzzb.put("PROTOCOL",sessionRecordLog.getCommon_schema_type());
        //dns_rzzb.put("YYS",sessionRecordLog.getCommon_isp());
        dns_rzzb.put("Yl1",sessionRecordLog.getCommon_address_type());

        //dns特有
        dns_rzzb.put("CSPKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_rzzb.put("SCBYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_rzzb.put("SCPKTS",sessionRecordLog.getCommon_s2c_byte_num());
        dns_rzzb.put("SCBYTES",sessionRecordLog.getCommon_s2c_byte_num());

        dns_rzzb.put("DNSNAME",sessionRecordLog.getDns_qname());
        dns_rzzb.put("DNSVALUE",sessionRecordLog.getDns_rr());
    }

}
