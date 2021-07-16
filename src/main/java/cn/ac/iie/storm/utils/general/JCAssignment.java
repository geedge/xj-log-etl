package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.GetMap;
import org.apache.avro.generic.GenericRecord;


import java.io.IOException;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.config.GetMap.*;
import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class JCAssignment {
    public static void setIP_JCRZ(GenericRecord ip_jcrz, SessionRecordLog sessionRecordLog) throws IOException {
        ip_jcrz.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ip_jcrz.put("PZID",sessionRecordLog.getPzid());
        ip_jcrz.put("FLID",sessionRecordLog.getFlid());
        ip_jcrz.put("XZID",sessionRecordLog.getXzid());
        ip_jcrz.put("LHID",sessionRecordLog.getLhid());
        ip_jcrz.put("PZLX",sessionRecordLog.getPzlx());
        ip_jcrz.put("GKLB",sessionRecordLog.getGklb());
        ip_jcrz.put("YL3",sessionRecordLog.getLabels());
        ip_jcrz.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ip_jcrz.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ip_jcrz.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ip_jcrz.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ip_jcrz.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ip_jcrz.put("SPORT",sessionRecordLog.getCommon_client_port());
        ip_jcrz.put("DPORT",sessionRecordLog.getCommon_server_port());
        ip_jcrz.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ip_jcrz.put("DETAIL_ID","08002"); //同上
        ip_jcrz.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ip_jcrz.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ip_jcrz.put("YL1",sessionRecordLog.getCommon_address_type());
        //dns特有
        ip_jcrz.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ip_jcrz.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ip_jcrz.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ip_jcrz.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ip_jcrz.put("HOST",sessionRecordLog.getHttp_host());
        ip_jcrz.put("URL",sessionRecordLog.getHttp_url());
        ip_jcrz.put("CONTENT",sessionRecordLog.getHttp_response_content());

    }

    public static void setDNS_JCZB(GenericRecord dns_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_jczb.put("PZID",sessionRecordLog.getPzid());
        dns_jczb.put("FLID",sessionRecordLog.getFlid());
        dns_jczb.put("XZID",sessionRecordLog.getXzid());
        dns_jczb.put("LHID",sessionRecordLog.getLhid());
        dns_jczb.put("PZLX",sessionRecordLog.getPzlx());
        dns_jczb.put("GKLB",sessionRecordLog.getGklb());
        dns_jczb.put("YL3",sessionRecordLog.getLabels() );
        dns_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        dns_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        dns_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        dns_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        dns_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        dns_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        dns_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        dns_jczb.put("DETAIL_ID","02999"); //同上
        dns_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        dns_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        //dns特有
        dns_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        dns_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        dns_jczb.put("DNS_NAME",getDns_name(sessionRecordLog.getDns_qname()));
        dns_jczb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
    }

    public static void setHTTP_JCZB(GenericRecord http_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        http_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        http_jczb.put("PZID",sessionRecordLog.getPzid());
        http_jczb.put("FLID",sessionRecordLog.getFlid());
        http_jczb.put("XZID",sessionRecordLog.getXzid());
        http_jczb.put("LHID",sessionRecordLog.getLhid());
        http_jczb.put("PZLX",sessionRecordLog.getPzlx());
        http_jczb.put("GKLB",sessionRecordLog.getGklb());
        http_jczb.put("YL3",sessionRecordLog.getLabels());
        http_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        http_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        http_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        http_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        http_jczb.put("DETAIL_ID","01999"); //同上
        http_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center()));  //-**common_isp这个字段
        http_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        http_jczb.put("YL5",sessionRecordLog.getHttp_response_body());
        //http特有
        http_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        http_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        http_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        http_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        http_jczb.put("HOST",sessionRecordLog.getHttp_host());
        http_jczb.put("URL",sessionRecordLog.getHttp_url());
        http_jczb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        http_jczb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        http_jczb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        http_jczb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
        http_jczb.put("YYZL",sessionRecordLog.getYyzl());

    }
    public static void setSSL_JCZB(GenericRecord ssl_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        ssl_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ssl_jczb.put("PZID",sessionRecordLog.getPzid());
        ssl_jczb.put("FLID",sessionRecordLog.getFlid());
        ssl_jczb.put("XZID",sessionRecordLog.getXzid());
        ssl_jczb.put("LHID",sessionRecordLog.getLhid());
        ssl_jczb.put("PZLX",sessionRecordLog.getPzlx());
        ssl_jczb.put("GKLB",sessionRecordLog.getGklb());
        ssl_jczb.put("YL3",sessionRecordLog.getLabels());
        ssl_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ssl_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ssl_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ssl_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ssl_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ssl_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ssl_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ssl_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ssl_jczb.put("DETAIL_ID","07999"); //同上
        ssl_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ssl_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ssl_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        //ssl特有
        ssl_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ssl_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ssl_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ssl_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ssl_jczb.put("FWQMC",sessionRecordLog.getSsl_sni());
        ssl_jczb.put("ZSMC",sessionRecordLog.getZsmc());

    }

   public static void setWYGJC_JCZB(GenericRecord wygjc_jczb, SessionRecordLog sessionRecordLog) {
       wygjc_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
       wygjc_jczb.put("PZID",sessionRecordLog.getPzid());
       wygjc_jczb.put("FLID",sessionRecordLog.getFlid());
       wygjc_jczb.put("XZID",sessionRecordLog.getXzid());
       wygjc_jczb.put("LHID",sessionRecordLog.getLhid());
       wygjc_jczb.put("PZLX",sessionRecordLog.getPzlx());
       wygjc_jczb.put("GKLB",sessionRecordLog.getGklb());
       wygjc_jczb.put("YL3",sessionRecordLog.getLabels());
       wygjc_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
       wygjc_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
       wygjc_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
       wygjc_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
       wygjc_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
       wygjc_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
       wygjc_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
       wygjc_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
       wygjc_jczb.put("DETAIL_ID","01999"); //同上
       wygjc_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
       wygjc_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
       wygjc_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
       wygjc_jczb.put("YL5",sessionRecordLog.getHttp_response_body());
        //wygjc特有
       wygjc_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
       wygjc_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
       wygjc_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
       wygjc_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
       wygjc_jczb.put("JC_URL",sessionRecordLog.getHttp_url());
       wygjc_jczb.put("SSYQ",sessionRecordLog.getHttp_host());
       wygjc_jczb.put("GJZ",sessionRecordLog.getGjc());
       wygjc_jczb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
       wygjc_jczb.put("YYZL",sessionRecordLog.getYyzl());
    }
    public static void  setMAIL_JCZB(GenericRecord mail_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_jczb.put("PZID",sessionRecordLog.getPzid());
        mail_jczb.put("FLID",sessionRecordLog.getFlid());
        mail_jczb.put("XZID",sessionRecordLog.getXzid());
        mail_jczb.put("LHID",sessionRecordLog.getLhid());
        mail_jczb.put("PZLX",sessionRecordLog.getPzlx());
        mail_jczb.put("GKLB",sessionRecordLog.getGklb());
        mail_jczb.put("YL3",sessionRecordLog.getLabels());
        mail_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        mail_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        mail_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        mail_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        if (sessionRecordLog.getPzlx()==410||sessionRecordLog.getPzlx()==510) {
            mail_jczb.put("DETAIL_ID",sessionRecordLog.getYjxy());
        }else {
            mail_jczb.put("DETAIL_ID",getMailDetail_id(sessionRecordLog.getMail_protocol_type()));
        }
        mail_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center()));
        mail_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        mail_jczb.put("YL5",sessionRecordLog.getHttp_response_body());
        mail_jczb.put("YL2",geSecuritytNum(sessionRecordLog.getMail_bcc()));
        mail_jczb.put("YL4",sessionRecordLog.getMail_bcc());

        //mail特有
        mail_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        mail_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_jczb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_jczb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_jczb.put("MAIL_RECEVER",sessionRecordLog.getMail_to());
        mail_jczb.put("RECEVER_NUM",geSecuritytNum(sessionRecordLog.getMail_to()));
        mail_jczb.put("MAIL_CC",sessionRecordLog.getMail_cc());
        mail_jczb.put("CC_NUM",geSecuritytNum(sessionRecordLog.getMail_cc()));
        mail_jczb.put("MAIL_SUBJECT",sessionRecordLog.getMail_subject());
        mail_jczb.put("CONTENT",sessionRecordLog.getMail_content());
        mail_jczb.put("ACCESSORY",getAccessory(sessionRecordLog.getMail_attachment_name()));
        mail_jczb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_jczb.put("FILE_CONTENT",sessionRecordLog.getMail_eml_file());
        mail_jczb.put("PWD",sessionRecordLog.getMail_passwd());
        mail_jczb.put("FJFWS",getSafeFws(sessionRecordLog.getMail_from()));
        mail_jczb.put("FJFWS_SZD",getFjSzd(sessionRecordLog.getCommon_client_location(),sessionRecordLog.getMail_from(),sessionRecordLog.getMail_protocol_type())); //  //境内境外未知，根据Location填充
        mail_jczb.put("SJFWS",getSafeFws(sessionRecordLog.getMail_to()));
        mail_jczb.put("SJFWS_SZD",getSjSzd(sessionRecordLog.getCommon_server_location(),sessionRecordLog.getMail_to(),sessionRecordLog.getMail_protocol_type()));//境内境外未知，根据Location填充
        if (sessionRecordLog.getPzlx()==410||sessionRecordLog.getPzlx()==510) {
            mail_jczb.put("XYLX", getYjxy(sessionRecordLog.getPzlx(), sessionRecordLog.getYjxy(), sessionRecordLog.getMail_protocol_type()));
        }else {
            mail_jczb.put("XYLX",sessionRecordLog.getMail_protocol_type());
        }
        mail_jczb.put("GJZ",sessionRecordLog.getGjc());
        mail_jczb.put("YYZL",sessionRecordLog.getYyzl());
    }

    public static void setFTP_JCZB(GenericRecord ftp_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_jczb.put("PZID",sessionRecordLog.getPzid());
        ftp_jczb.put("FLID",sessionRecordLog.getFlid());
        ftp_jczb.put("XZID",sessionRecordLog.getXzid());
        ftp_jczb.put("LHID",sessionRecordLog.getLhid());
        ftp_jczb.put("PZLX",sessionRecordLog.getPzlx());
        ftp_jczb.put("GKLB",sessionRecordLog.getGklb());
        ftp_jczb.put("YL3",sessionRecordLog.getLabels());
        ftp_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ftp_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ftp_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ftp_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ftp_jczb.put("DETAIL_ID","06999"); //同上
        ftp_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));   //-common_l4_protocol其实是这个字段
        ftp_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ftp_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        //ftp特有
        ftp_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ftp_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ftp_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ftp_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ftp_jczb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_jczb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
        ftp_jczb.put("GJZ",sessionRecordLog.getGjc());
        ftp_jczb.put("YYZL",sessionRecordLog.getYyzl());
    }


    public static void setVPN_JCZB(GenericRecord vpn_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        vpn_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_jczb.put("PZID",sessionRecordLog.getPzid());
        vpn_jczb.put("FLID",sessionRecordLog.getFlid());
        vpn_jczb.put("XZID",sessionRecordLog.getXzid());
        vpn_jczb.put("LHID",sessionRecordLog.getLhid());
        vpn_jczb.put("PZLX",sessionRecordLog.getPzlx());
        vpn_jczb.put("GKLB",sessionRecordLog.getGklb());
        vpn_jczb.put("YL3",sessionRecordLog.getLabels());
        vpn_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        vpn_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        vpn_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        vpn_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));
        vpn_jczb.put("DETAIL_ID",getVpnDetail_id(sessionRecordLog.getCommon_l7_protocol(),sessionRecordLog.getCommon_protocol_label()));
        vpn_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));
        vpn_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        vpn_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        //vpn特有
        vpn_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        vpn_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        vpn_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        vpn_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
    }

    public static void setJSTX_JCZB(GenericRecord jstx_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_jczb.put("PZID",sessionRecordLog.getPzid());
        jstx_jczb.put("FLID",sessionRecordLog.getFlid());
        jstx_jczb.put("XZID",sessionRecordLog.getXzid());
        jstx_jczb.put("LHID",sessionRecordLog.getLhid());
        jstx_jczb.put("PZLX",sessionRecordLog.getPzlx());
        jstx_jczb.put("GKLB",sessionRecordLog.getGklb());
        jstx_jczb.put("YL3",sessionRecordLog.getLabels());
        jstx_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        jstx_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        jstx_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        jstx_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        jstx_jczb.put("DETAIL_ID", sessionRecordLog.getProtocol());
        jstx_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center()));//-**common_isp这个字段
        jstx_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        //http特有
        jstx_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        jstx_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_jczb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_jczb.put("URL",sessionRecordLog.getHttp_url());
        jstx_jczb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_jczb.put("YYZL",sessionRecordLog.getYyzl());
    }

    public static void setPIC_JCZB(GenericRecord pic_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        pic_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        pic_jczb.put("PZID",sessionRecordLog.getPzid());
        pic_jczb.put("FLID",sessionRecordLog.getFlid());
        pic_jczb.put("XZID",sessionRecordLog.getXzid());
        pic_jczb.put("LHID",sessionRecordLog.getLhid());
        pic_jczb.put("PZLX",sessionRecordLog.getPzlx());
        pic_jczb.put("GKLB",sessionRecordLog.getGklb());
        pic_jczb.put("YL3",sessionRecordLog.getLabels());
        pic_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        pic_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        pic_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        pic_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        pic_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        pic_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        pic_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        pic_jczb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        pic_jczb.put("DETAIL_ID","01999"); //同上
        pic_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        pic_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        pic_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        pic_jczb.put("YL5",sessionRecordLog.getHttp_response_body());
        //pic特有
        pic_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        pic_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        pic_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        pic_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        pic_jczb.put("PIC_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        pic_jczb.put("PIC_URL",sessionRecordLog.getHttp_url());
        pic_jczb.put("PIC_CONTENT",sessionRecordLog.getHttp_snapshot());
        pic_jczb.put("PIC_TYPE",sessionRecordLog.getHttp_content_type());
        pic_jczb.put("YYZL",sessionRecordLog.getYyzl());

    }
    public static void setMEDIA_JCZB(GenericRecord media_jczb, SessionRecordLog sessionRecordLog) throws IOException {
        media_jczb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        media_jczb.put("PZID",sessionRecordLog.getPzid());
        media_jczb.put("FLID",sessionRecordLog.getFlid());
        media_jczb.put("XZID",sessionRecordLog.getXzid());
        media_jczb.put("LHID",sessionRecordLog.getLhid());
        media_jczb.put("PZLX",sessionRecordLog.getPzlx());
        media_jczb.put("GKLB",sessionRecordLog.getGklb());
        media_jczb.put("YL3",sessionRecordLog.getLabels());
        media_jczb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        media_jczb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        media_jczb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        media_jczb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        media_jczb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        media_jczb.put("SPORT",sessionRecordLog.getCommon_client_port());
        media_jczb.put("DPORT",sessionRecordLog.getCommon_server_port());
        media_jczb.put("PROTO_ID", getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        media_jczb.put("DETAIL_ID","01999"); //同上
        media_jczb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        media_jczb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        media_jczb.put("YL1",sessionRecordLog.getCommon_address_type());
        media_jczb.put("YL5",sessionRecordLog.getHttp_response_body());
        //media特有
        media_jczb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        media_jczb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        media_jczb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        media_jczb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        media_jczb.put("MEDIA_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        media_jczb.put("MEDIA_URL",sessionRecordLog.getHttp_url());
//        media_jczb.put("MEDIA_CONTENT",sessionRecordLog.getHttp_response_body());
        media_jczb.put("MEDIA_TYPE",sessionRecordLog.getHttp_content_type());
        media_jczb.put("YYZL",sessionRecordLog.getYyzl());
    }
}
