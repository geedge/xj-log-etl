package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.GetMap;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;

import java.io.IOException;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;


import static cn.ac.iie.storm.utils.config.GetMap.*;

import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.MailTrans.getNum;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class CONNAssignment {
    private final static Logger logger = Logger.getLogger(CONNAssignment.class);
    public static void setDNS_RZZB(GenericRecord dns_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        dns_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        dns_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        dns_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        dns_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        dns_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        dns_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        dns_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        dns_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        dns_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
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
        IP_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        IP_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        IP_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        IP_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        IP_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        IP_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        IP_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        IP_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
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

           http_rzzb.put("YDY", getDy(sessionRecordLog.getCommon_client_location()));
           http_rzzb.put("MDDY", getDy(sessionRecordLog.getCommon_server_location()));
           http_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
           http_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(), sessionRecordLog.getCommon_client_ip()));
           http_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(), sessionRecordLog.getCommon_server_ip()));
           http_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
           http_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
           http_rzzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
           http_rzzb.put("DETAIL_ID","01999"); //同上
           http_rzzb.put("PROTOCOL", getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
           http_rzzb.put("YYS", getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
           http_rzzb.put("YL1", sessionRecordLog.getCommon_address_type());
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
        vpn_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        vpn_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
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
        ssl_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ssl_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
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

        ssl_rzzb.put("FWQMC",sessionRecordLog.getSsl_sni());
    }

    public static void setFTP_RZZB(GenericRecord ftp_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ftp_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
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
        mail_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        mail_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
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
        mail_rzzb.put("FJFWS_SZD",getSzd(sessionRecordLog.getCommon_client_location())); //  //境内境外未知，根据Location填充

        mail_rzzb.put("SJFWS",sessionRecordLog.getMail_to());
        mail_rzzb.put("SJFWS_SZD",getSzd(sessionRecordLog.getCommon_server_location()));//境内境外未知，根据Location填充


        mail_rzzb.put("YL2",getNum(sessionRecordLog.getMail_bcc()));
        mail_rzzb.put("YL4",getMailRecever(sessionRecordLog.getMail_bcc()));

        mail_rzzb.put("XYLX",sessionRecordLog.getMail_protocol_type());
    }

    public static void setJSTX_RZZB(GenericRecord jstx_rzzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_rzzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_rzzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        jstx_rzzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        jstx_rzzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_server_ip())); //--其实是使用网络序列
        jstx_rzzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_rzzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_rzzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_rzzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_rzzb.put("PROTO_ID", GetMap.getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        jstx_rzzb.put("DETAIL_ID", GetMap.getJstxDetail_id(sessionRecordLog.getCommon_app_id())); //同上
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
        jstx_rzzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
    }
}
