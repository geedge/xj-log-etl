package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.GetMap;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.config.GetMap.*;
import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.MailTrans.getFjfws;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getYys;

public class GKAssignment {
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
        ip_gkrz.put("YL3",sessionRecordLog.getLabels());
        ip_gkrz.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ip_gkrz.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ip_gkrz.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ip_gkrz.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ip_gkrz.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ip_gkrz.put("SPORT",sessionRecordLog.getCommon_client_port());
        ip_gkrz.put("DPORT",sessionRecordLog.getCommon_server_port());
        ip_gkrz.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ip_gkrz.put("DETAIL_ID","08001"); //同上
        ip_gkrz.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ip_gkrz.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ip_gkrz.put("YL1",sessionRecordLog.getCommon_address_type());
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
        dns_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        dns_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        dns_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
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
        dns_gkzb.put("DNS_NAME",getDns_name(sessionRecordLog.getDns_qname()));
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
        http_gkzb.put("YL3",sessionRecordLog.getLabels());
        http_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        http_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        http_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        http_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        http_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        http_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        http_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        http_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        http_gkzb.put("DETAIL_ID","01999"); //同上
        http_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        http_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        http_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        http_gkzb.put("YL5",sessionRecordLog.getHttp_response_body());
        //http特有
        http_gkzb.put("HOST",sessionRecordLog.getHttp_host());
        http_gkzb.put("URL",sessionRecordLog.getHttp_url());
        http_gkzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        http_gkzb.put("COOKIE",sessionRecordLog.getHttp_cookie());
        http_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getHttp_url()));
        http_gkzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
        http_gkzb.put("YYZL",sessionRecordLog.getYyzl());

    }

    public static void setSSL_GKZB(GenericRecord ssl_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        ssl_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ssl_gkzb.put("PZID",sessionRecordLog.getPzid());
        ssl_gkzb.put("FLID",sessionRecordLog.getFlid());
        ssl_gkzb.put("XZID",sessionRecordLog.getXzid());
        ssl_gkzb.put("LHID",sessionRecordLog.getLhid());
        ssl_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        ssl_gkzb.put("GKLB",sessionRecordLog.getGklb());
        ssl_gkzb.put("YL3",sessionRecordLog.getLabels());
        ssl_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ssl_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ssl_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ssl_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ssl_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ssl_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ssl_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ssl_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ssl_gkzb.put("DETAIL_ID","07999"); //同上
        ssl_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        ssl_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ssl_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //ssl特有
        ssl_gkzb.put("FWQMC",sessionRecordLog.getSsl_sni());
        ssl_gkzb.put("ZSMC",sessionRecordLog.getZsmc());

    }

    public static void setWYGJC_GKZB(GenericRecord wygjc_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        wygjc_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        wygjc_gkzb.put("PZID",sessionRecordLog.getPzid());
        wygjc_gkzb.put("FLID",sessionRecordLog.getFlid());
        wygjc_gkzb.put("XZID",sessionRecordLog.getXzid());
        wygjc_gkzb.put("LHID",sessionRecordLog.getLhid());
        wygjc_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        wygjc_gkzb.put("GKLB",sessionRecordLog.getGklb());
        wygjc_gkzb.put("YL3",sessionRecordLog.getLabels());
        wygjc_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        wygjc_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        wygjc_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        wygjc_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        wygjc_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        wygjc_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        wygjc_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        wygjc_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        wygjc_gkzb.put("DETAIL_ID","01999"); //同上
        wygjc_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        wygjc_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        wygjc_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        wygjc_gkzb.put("YL5",sessionRecordLog.getHttp_response_body());
        //wygjc特有
        wygjc_gkzb.put("JC_URL",sessionRecordLog.getHttp_url());
        wygjc_gkzb.put("SSYQ",sessionRecordLog.getHttp_domain());
        wygjc_gkzb.put("CONTENT",sessionRecordLog.getHttp_snapshot());
        wygjc_gkzb.put("YYZL",sessionRecordLog.getYyzl());
        wygjc_gkzb.put("GJZ",sessionRecordLog.getGjc());
    }

    public static void setMAIL_GKZB(GenericRecord mail_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        mail_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        mail_gkzb.put("PZID",sessionRecordLog.getPzid());
        mail_gkzb.put("FLID",sessionRecordLog.getFlid());
        mail_gkzb.put("XZID",sessionRecordLog.getXzid());
        mail_gkzb.put("LHID",sessionRecordLog.getLhid());
        mail_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        mail_gkzb.put("GKLB",sessionRecordLog.getGklb());
        mail_gkzb.put("YL3",sessionRecordLog.getLabels());
        mail_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        mail_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        mail_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        mail_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        mail_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        mail_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        mail_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        mail_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        mail_gkzb.put("DETAIL_ID",getMailDetail_id(sessionRecordLog.getMail_protocol_type()));
        mail_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        mail_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        mail_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        mail_gkzb.put("YL5",sessionRecordLog.getHttp_response_body());

        mail_gkzb.put("YL2",geSecuritytNum(sessionRecordLog.getMail_bcc()));
        mail_gkzb.put("YL4",getMailRecever(sessionRecordLog.getMail_bcc()));

        //mail特有

        mail_gkzb.put("MAIL_SENDER",sessionRecordLog.getMail_from());

        mail_gkzb.put("MAIL_RECEVER",sessionRecordLog.getMail_to());

        mail_gkzb.put("RECEVER_NUM",geSecuritytNum(sessionRecordLog.getMail_to()));

        mail_gkzb.put("MAIL_CC",sessionRecordLog.getMail_cc());
        mail_gkzb.put("CC_NUM",geSecuritytNum(sessionRecordLog.getMail_cc()));

        mail_gkzb.put("MAIL_SUBJECT",sessionRecordLog.getMail_subject());

        mail_gkzb.put("CONTENT",sessionRecordLog.getMail_content());

        mail_gkzb.put("ACCESSORY",getAccessory(sessionRecordLog.getMail_attachment_name()));

        mail_gkzb.put("FILE_NAME",sessionRecordLog.getMail_attachment_name());
        mail_gkzb.put("FILE_CONTENT",sessionRecordLog.getMail_eml_file());
        mail_gkzb.put("PWD",sessionRecordLog.getMail_passwd());
        mail_gkzb.put("FJFWS",getFjfws(sessionRecordLog.getMail_from()));
        mail_gkzb.put("FJFWS_SZD",getSzd(sessionRecordLog.getCommon_client_location())); //  //境内境外未知，根据Location填充

        mail_gkzb.put("SJFWS",sessionRecordLog.getMail_to());
        mail_gkzb.put("SJFWS_SZD",getSzd(sessionRecordLog.getCommon_server_location()));//境内境外未知，根据Location填充


        mail_gkzb.put("XYLX",sessionRecordLog.getMail_protocol_type());

        mail_gkzb.put("GJZ",sessionRecordLog.getGjc());
        mail_gkzb.put("YYZL",sessionRecordLog.getYyzl());

    }


    public static void setFTP_GKZB(GenericRecord ftp_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        ftp_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        ftp_gkzb.put("PZID",sessionRecordLog.getPzid());
        ftp_gkzb.put("FLID",sessionRecordLog.getFlid());
        ftp_gkzb.put("XZID",sessionRecordLog.getXzid());
        ftp_gkzb.put("LHID",sessionRecordLog.getLhid());
        ftp_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        ftp_gkzb.put("GKLB",sessionRecordLog.getGklb());
        ftp_gkzb.put("YL3",sessionRecordLog.getLabels());
        ftp_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        ftp_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        ftp_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        ftp_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        ftp_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        ftp_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        ftp_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        ftp_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        ftp_gkzb.put("DETAIL_ID","06999"); //同上
        ftp_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));   //-common_l4_protocol其实是这个字段
        ftp_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        ftp_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //ftp特有
        ftp_gkzb.put("FILE_NAME",getFile_Name(sessionRecordLog.getFtp_url()));
        ftp_gkzb.put("FILE_CONTENT",sessionRecordLog.getFtp_url());
        ftp_gkzb.put("GJZ",sessionRecordLog.getGjc());
        ftp_gkzb.put("YYZL",sessionRecordLog.getYyzl());
    }


    public static void setVPN_GKZB(GenericRecord vpn_gkzb, SessionRecordLog sessionRecordLog) throws IOException {

        vpn_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        vpn_gkzb.put("PZID",sessionRecordLog.getPzid());
        vpn_gkzb.put("FLID",sessionRecordLog.getFlid());
        vpn_gkzb.put("XZID",sessionRecordLog.getXzid());
        vpn_gkzb.put("LHID",sessionRecordLog.getLhid());
        vpn_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        vpn_gkzb.put("GKLB",sessionRecordLog.getGklb());
        vpn_gkzb.put("YL3",sessionRecordLog.getLabels());
        vpn_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        vpn_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        vpn_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        vpn_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        vpn_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        vpn_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        vpn_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        vpn_gkzb.put("PROTO_ID",getProto_id(sessionRecordLog.getCommon_schema_type()));  ///--转化表 还没弄
        vpn_gkzb.put("DETAIL_ID",getVpnDetail_id(sessionRecordLog.getCommon_l7_protocol())); //同上
        vpn_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        vpn_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段
        vpn_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //vpn特有

    }


    public static void setJSTX_GKZB(GenericRecord jstx_gkzb, SessionRecordLog sessionRecordLog) throws IOException {
        jstx_gkzb.put("BHSJ",sessionRecordLog.getCommon_recv_time());
        jstx_gkzb.put("PZID",sessionRecordLog.getPzid());
        jstx_gkzb.put("FLID",sessionRecordLog.getFlid());
        jstx_gkzb.put("XZID",sessionRecordLog.getXzid());
        jstx_gkzb.put("LHID",sessionRecordLog.getLhid());
        jstx_gkzb.put("PZLX",sessionRecordLog.getPzlx());
        jstx_gkzb.put("GKLB",sessionRecordLog.getGklb());
        jstx_gkzb.put("YL3",sessionRecordLog.getLabels());
        jstx_gkzb.put("YDY",getDy(sessionRecordLog.getCommon_client_location()));
        jstx_gkzb.put("MDDY",getDy(sessionRecordLog.getCommon_server_location()));
        jstx_gkzb.put("QDJIP",getQDJIP(sessionRecordLog.getCommon_sled_ip())); //--其实是使用网络序列
        jstx_gkzb.put("SIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_client_ip()));
        jstx_gkzb.put("DIP",getIP(sessionRecordLog.getCommon_address_type(),sessionRecordLog.getCommon_server_ip()));
        jstx_gkzb.put("SPORT",sessionRecordLog.getCommon_client_port());
        jstx_gkzb.put("DPORT",sessionRecordLog.getCommon_server_port());
        jstx_gkzb.put("PROTO_ID", GetMap.getProto_id(sessionRecordLog.getCommon_schema_type()));
        jstx_gkzb.put("DETAIL_ID", GetMap.getJstxDetail_id(sessionRecordLog.getCommon_app_id()));
        jstx_gkzb.put("PROTOCOL",getProtocol(sessionRecordLog.getCommon_l4_protocol()));  //-common_l4_protocol其实是这个字段
        jstx_gkzb.put("YYS",getYys(sessionRecordLog.getCommon_data_center())); //-**common_isp这个字段 //getYys(sessionRecordLog.getCommon_isp())
        jstx_gkzb.put("YL1",sessionRecordLog.getCommon_address_type());
        //http特有
        jstx_gkzb.put("HOST",sessionRecordLog.getHttp_host());
        jstx_gkzb.put("URL",sessionRecordLog.getHttp_url());
        jstx_gkzb.put("USER_AGENT",sessionRecordLog.getHttp_user_agent());
        jstx_gkzb.put("YYZL",sessionRecordLog.getYyzl());
    }

}
