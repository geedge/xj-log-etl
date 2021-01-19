package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.GetAppId;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;


import static cn.ac.iie.storm.utils.config.GetAppId.getJstxDetail_id;
import static cn.ac.iie.storm.utils.config.GetAppId.getProto_id;
import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.MailTrans.getNum;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class Assignment {
    public static void setDNS_RZZB(GenericRecord dns_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        dns_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        dns_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        dns_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        dns_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        dns_rzzb.put("PROTO_ID","08");  ///--转化表 还没弄
        dns_rzzb.put("DETAIL_ID","02999"); //同上
        dns_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        dns_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //dns特有
        dns_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        dns_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        dns_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        dns_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        dns_rzzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        dns_rzzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
    }

    public static void setIP_RZZB(GenericRecord IP_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        IP_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        IP_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        IP_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        IP_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        IP_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        IP_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        IP_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        IP_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        IP_rzzb.put("PROTO_ID","");  ///--转化表 还没弄
        IP_rzzb.put("DETAIL_ID","08003"); //同上
        IP_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        IP_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        IP_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //dns特有
        IP_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        IP_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        IP_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        IP_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
    }

    public static void setHTTP_RZZB(GenericRecord http_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        http_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        http_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        http_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        http_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        http_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        http_rzzb.put("DETAIL_ID","01999"); //同上
        http_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        http_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //http特有
        http_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        http_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        http_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        http_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        http_rzzb.put("HOST",sessionRecordLog.getHttp_host());
        http_rzzb.put("URL",sessionRecordLog.getHttp_url());
        http_rzzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        http_rzzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        http_rzzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        http_rzzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
    }
    public static void setVPN_RZZB(GenericRecord vpn_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        vpn_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        vpn_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        vpn_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        vpn_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_rzzb.put("PROTO_ID","04");  ///--转化表 还没弄
        vpn_rzzb.put("DETAIL_ID",getVpnDetail_id(sessionRecordLog.getCommon_l7_protocol())); //同上
        vpn_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        vpn_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        vpn_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //vpn特有
        vpn_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        vpn_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        vpn_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        vpn_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
    }

    public static void setSSL_RZZB(GenericRecord ssl_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        ssl_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ssl_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ssl_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ssl_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ssl_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ssl_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ssl_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ssl_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ssl_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ssl_rzzb.put("DETAIL_ID","07999"); //同上
        ssl_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ssl_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        ssl_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());

        //ssl特有
        ssl_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ssl_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ssl_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ssl_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
    }

    public static void setFTP_RZZB(GenericRecord ftp_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ftp_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ftp_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ftp_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ftp_rzzb.put("DETAIL_ID","06999"); //同上
        ftp_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ftp_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        ftp_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());

        //ftp特有
        ftp_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ftp_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ftp_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ftp_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ftp_rzzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_rzzb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
    }

    public static void setMAIL_RZZB(GenericRecord mail_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        mail_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        mail_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        mail_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        mail_rzzb.put("DETAIL_ID",getMailDetail_id(sessionRecordLog.getMail_protocol_type())); //同上
        mail_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        mail_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //根据mail_bcc填充

        //mail特有
        mail_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        mail_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());

        mail_rzzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());


        mail_rzzb.put("MAIL_RECEVER", getMailRecever(sessionRecordLog.getMail_to()));
        mail_rzzb.put("RECEVER_NUM", getNum(sessionRecordLog.getMail_to()));

        mail_rzzb.put("MAIL_CC",sessionRecordLog.getMail_cc());


        mail_rzzb.put("CC_NUM", getNum(sessionRecordLog.getMail_cc()));

        mail_rzzb.put("CONTENT",sessionRecordLog.getMail_subject());
        mail_rzzb.put("MAIL_RECEVER",sessionRecordLog.getMail_content());

        mail_rzzb.put("ACCESSORY", getAccessory(sessionRecordLog.getMail_attachment_name()));

        mail_rzzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_rzzb.put("FILE_CONTENT",sessionRecordLog.getMail_attachment_content());
        mail_rzzb.put("PWD",sessionRecordLog.getMail_passwd());

        mail_rzzb.put("FJFWS",getFjfws(sessionRecordLog.getMail_from()));


        mail_rzzb.put("FJFWS_SZD",sessionRecordLog.getCommon_client_location()); //  //境内境外未知，根据Location填充
        mail_rzzb.put("SJFWS",sessionRecordLog.getCommon_server_location());
        mail_rzzb.put("SJFWS_SZD",sessionRecordLog.getMail_content());//境内境外未知，根据Location填充

        mail_rzzb.put("YL2",getNum(sessionRecordLog.getMail_bcc()));
        mail_rzzb.put("YL4",getMailRecever(sessionRecordLog.getMail_bcc()));

        mail_rzzb.put("XYLX",sessionRecordLog.getMail_protocol_type());
    }

    public static void setJSTX_RZZB(GenericRecord jstx_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_rzzb.put("YDY",sessionRecordLog.getCommon_client_location());
        jstx_rzzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        jstx_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_rzzb.put("PROTO_ID",GetAppId.getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        jstx_rzzb.put("DETAIL_ID", GetAppId.getJstxDetail_id(sessionRecordLog.getCommon_app_id())); //同上
        jstx_rzzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_rzzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 getYys(sessionRecordLog.getCommon_isp())//
        jstx_rzzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //ftp特有
        jstx_rzzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_rzzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_rzzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        jstx_rzzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_rzzb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_rzzb.put("URL",sessionRecordLog.getHttp_url());
 
        jstx_rzzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
    }


    /**
     *gk类
     */
    public static void setIP_GKRZ(GenericRecord ip_gkrz, SessionRecordLog sessionRecordLog) throws IOException {
        ip_gkrz.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ip_gkrz.put("PZID",sessionRecordLog.getPzid());
        ip_gkrz.put("FLID",sessionRecordLog.getFlid());
        ip_gkrz.put("XZID",sessionRecordLog.getXzid());
        ip_gkrz.put("LHID",sessionRecordLog.getLhid());
        ip_gkrz.put("PZLX",sessionRecordLog.getPzlx());
        ip_gkrz.put("GKLB",sessionRecordLog.getGklb());
        ip_gkrz.put("yl3",sessionRecordLog.getLabels());
        ip_gkrz.put("YDY",sessionRecordLog.getCommon_client_location());
        ip_gkrz.put("MDDY",sessionRecordLog.getCommon_server_location());
        ip_gkrz.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ip_gkrz.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ip_gkrz.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ip_gkrz.put("SPORT",sessionRecordLog.getCommon_client_port());
        ip_gkrz.put("DPORT",sessionRecordLog.getCommon_server_port());
        ip_gkrz.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ip_gkrz.put("DETAIL_ID","1"); //同上
        ip_gkrz.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ip_gkrz.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ip_gkrz.put("yl1",sessionRecordLog.getCommon_address_type());

    }

    public static void setDNS_GKZB(GenericRecord dns_gkzb, SessionRecordLog sessionRecordLog) throws IOException {

        dns_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_gkzb.put("PZID",sessionRecordLog.getPzid());
        dns_gkzb.put("FLID",sessionRecordLog.getFlid());
        dns_gkzb.put("XZID",sessionRecordLog.getXzid());
        dns_gkzb.put("LHID",sessionRecordLog.getLhid());
        dns_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        dns_gkzb.put("GKLB",sessionRecordLog.getGklb());
        dns_gkzb.put("YL3",sessionRecordLog.getLabels());
        dns_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        dns_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        dns_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        dns_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        dns_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        dns_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        dns_gkzb.put("DETAIL_ID","02999"); //同上
        dns_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        dns_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        dns_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //dns特有
        dns_gkzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        dns_gkzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
    }


    public static  void setHTTP_GKZB(GenericRecord http_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        http_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        http_gkzb.put("PZID",sessionRecordLog.getPzid());
        http_gkzb.put("FLID",sessionRecordLog.getFlid());
        http_gkzb.put("XZID",sessionRecordLog.getXzid());
        http_gkzb.put("LHID",sessionRecordLog.getLhid());
        http_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        http_gkzb.put("GKLB",sessionRecordLog.getGklb());
        http_gkzb.put("yl3",sessionRecordLog.getLabels());
        http_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        http_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        http_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        http_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        http_gkzb.put("DETAIL_ID","1"); //同上
        http_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        http_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //http特有
        http_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        http_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        http_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        http_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        http_gkzb.put("URL",sessionRecordLog.getHttp_url());
        http_gkzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        http_gkzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        http_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        http_gkzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
    }

    public static void setSSL_GKZB(GenericRecord ssl_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        ssl_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ssl_gkzb.put("PZID",sessionRecordLog.getPzid());
        ssl_gkzb.put("FLID",sessionRecordLog.getFlid());
        ssl_gkzb.put("XZID",sessionRecordLog.getXzid());
        ssl_gkzb.put("LHID",sessionRecordLog.getLhid());
        ssl_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        ssl_gkzb.put("GKLB",sessionRecordLog.getGklb());
        ssl_gkzb.put("yl3",sessionRecordLog.getLabels());
        ssl_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ssl_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ssl_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ssl_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ssl_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ssl_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ssl_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ssl_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ssl_gkzb.put("DETAIL_ID","1"); //同上
        ssl_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ssl_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ssl_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //ssl特有
        ssl_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ssl_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ssl_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        ssl_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ssl_gkzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        ssl_gkzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
    }

    public static void setWYGJC_GKZB(GenericRecord wygjc_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        wygjc_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        wygjc_gkzb.put("PZID",sessionRecordLog.getPzid());
        wygjc_gkzb.put("FLID",sessionRecordLog.getFlid());
        wygjc_gkzb.put("XZID",sessionRecordLog.getXzid());
        wygjc_gkzb.put("LHID",sessionRecordLog.getLhid());
        wygjc_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        wygjc_gkzb.put("GKLB",sessionRecordLog.getGklb());
        wygjc_gkzb.put("yl3",sessionRecordLog.getLabels());
        wygjc_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        wygjc_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        wygjc_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        wygjc_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        wygjc_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        wygjc_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        wygjc_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        wygjc_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        wygjc_gkzb.put("DETAIL_ID","1"); //同上
        wygjc_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        wygjc_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_isp())); //-**common_isp这个字段
        wygjc_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //wygjc特有
        wygjc_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        wygjc_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        wygjc_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        wygjc_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        wygjc_gkzb.put("DNS_NAME",sessionRecordLog.getDns_qname());
        wygjc_gkzb.put("DNS_VALUE",sessionRecordLog.getDns_rr());
    }

    public static void setMAIL_GKZB(GenericRecord mail_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_gkzb.put("PZID",sessionRecordLog.getPzid());
        mail_gkzb.put("FLID",sessionRecordLog.getFlid());
        mail_gkzb.put("XZID",sessionRecordLog.getXzid());
        mail_gkzb.put("LHID",sessionRecordLog.getLhid());
        mail_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        mail_gkzb.put("GKLB",sessionRecordLog.getGklb());
        mail_gkzb.put("yl3",sessionRecordLog.getLabels());
        mail_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        mail_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        mail_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        mail_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        mail_gkzb.put("DETAIL_ID","1"); //同上
        mail_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        mail_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //根据mail_bcc填充
        /* mail_rzzb.put("yl2",sessionRecordLog.getMail_bcc);*/
        /*mail_rzzb.put("yl4",sessionRecordLog.getMail_bcc);*/

        //mail特有
        mail_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        mail_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        mail_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        mail_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        mail_gkzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());

        mail_gkzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());
        mail_gkzb.put("MAIL_RECEVER",getMailRecever(sessionRecordLog.getMail_to()));
        mail_gkzb.put("RECEVER_NUM",getNum(sessionRecordLog.getMail_to()));

        mail_gkzb.put("MAIL_CC",sessionRecordLog.getMail_cc());
        mail_gkzb.put("CC_NUM",getNum(sessionRecordLog.getMail_cc()));

        mail_gkzb.put("CONTENT",sessionRecordLog.getMail_subject());
        mail_gkzb.put("MAIL_RECEVER",sessionRecordLog.getMail_content());
        mail_gkzb.put("ACCESSORY",getAccessory(sessionRecordLog.getMail_attachment_name()));

        mail_gkzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_gkzb.put("FILE_CONTENT",sessionRecordLog.getMail_attachment_content());
        mail_gkzb.put("PWD",sessionRecordLog.getMail_passwd());
        mail_gkzb.put("FJFWS",getFjfws(sessionRecordLog.getMail_from()));
        //mail_rzzb.put("FJFWS_SZD",sessionRecordLog.getMail_content()); //  //境内境外未知，根据Location填充
        mail_gkzb.put("SJFWS",getFjfws(sessionRecordLog.getMail_to()));
        //mail_rzzb.put("SJFWS_SZD",sessionRecordLog.getMail_content());//境内境外未知，根据Location填充
        mail_gkzb.put("XYLX",sessionRecordLog.getMail_protocol_type());
    }


    public static void setFTP_GKZB(GenericRecord ftp_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_gkzb.put("PZID",sessionRecordLog.getPzid());
        ftp_gkzb.put("FLID",sessionRecordLog.getFlid());
        ftp_gkzb.put("XZID",sessionRecordLog.getXzid());
        ftp_gkzb.put("LHID",sessionRecordLog.getLhid());
        ftp_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        ftp_gkzb.put("GKLB",sessionRecordLog.getGklb());
        ftp_gkzb.put("yl3",sessionRecordLog.getLabels());
        ftp_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        ftp_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        ftp_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        ftp_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ftp_gkzb.put("DETAIL_ID","1"); //同上
        ftp_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ftp_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ftp_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //ftp特有
        ftp_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        ftp_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        ftp_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        ftp_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        ftp_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_gkzb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
    }


    public static void setVPN_GKZB(GenericRecord vpn_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        vpn_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_gkzb.put("PZID",sessionRecordLog.getPzid());
        vpn_gkzb.put("FLID",sessionRecordLog.getFlid());
        vpn_gkzb.put("XZID",sessionRecordLog.getXzid());
        vpn_gkzb.put("LHID",sessionRecordLog.getLhid());
        vpn_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        vpn_gkzb.put("GKLB",sessionRecordLog.getGklb());
        vpn_gkzb.put("yl3",sessionRecordLog.getLabels());
        vpn_gkzb.put("YDY",sessionRecordLog.getCommon_client_location());
        vpn_gkzb.put("MDDY",sessionRecordLog.getCommon_server_location());
        vpn_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        vpn_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        vpn_gkzb.put("DETAIL_ID","1"); //同上
        vpn_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        vpn_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        vpn_gkzb.put("yl1",sessionRecordLog.getCommon_address_type());
        //vpn特有
        vpn_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        vpn_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        vpn_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_pkt_num());
        vpn_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
    }


    public static void setJSTX_GKZB(GenericRecord jstx_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_gkzb.put("YDY","null");
        jstx_gkzb.put("MDDY","null");
        jstx_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_gkzb.put("PROTO_ID","08");  ///--转化表 还没弄
        jstx_gkzb.put("DETAIL_ID","02999"); //同上
        jstx_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        jstx_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //http特有
        jstx_gkzb.put("CS_PKTS",sessionRecordLog.getCommon_c2s_pkt_num());
        jstx_gkzb.put("CS_BYTES",sessionRecordLog.getCommon_c2s_byte_num());
        jstx_gkzb.put("SC_PKTS",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_gkzb.put("SC_BYTES",sessionRecordLog.getCommon_s2c_byte_num());
        jstx_gkzb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_gkzb.put("URL",sessionRecordLog.getHttp_url());
        jstx_gkzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_gkzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        jstx_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        jstx_gkzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());

    }


}
