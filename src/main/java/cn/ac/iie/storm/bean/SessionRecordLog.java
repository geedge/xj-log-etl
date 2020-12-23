package cn.ac.iie.storm.bean;

/**
 * @author wangchengcheng
 */
public class SessionRecordLog  {
    private long common_recv_time;
    private String common_l4_protocol;
    private String common_client_location;
    private String common_server_location;
    private String common_sled_ip;
    private String common_client_ip;
    private String common_server_ip;
    private int common_client_port;
    private int common_server_port;
    private String common_schema_type;
    private String common_isp;
    private int common_address_type;
    private String common_app_label;
    private String common_app_id;
    private int common_c2s_pkt_num;
    private long common_c2s_byte_num;
    private int common_s2c_pkt_num;
    private long common_s2c_byte_num;
    private String common_l7_protocol;
    private String dns_qname;
    private String dns_rr;
    private String 	ftp_url;
    private String 	ftp_content;
    private String 	http_host;
    private String 	http_url;
    private String 	http_user_agent;
    private String 	http_cookie;
    private String 	http_snapshot;
    private String 	ssl_sni;
    private long  recv_time;
    private String  mail_from;
    private String 	mail_to;
    private String mail_cc;
    private String mail_subject;
    private String mail_content;
    private String mail_attachment_name;
    private String mail_attachment_content;
    private String mail_passwd;
    private String mail_protocol_type;
    private String mail_snapshot;
    private String mail_bcc;

    public String getMail_bcc() {
        return mail_bcc;
    }

    public void setMail_bcc(String mail_bcc) {
        this.mail_bcc = mail_bcc;
    }

    public String getCommon_l4_protocol() {
        return common_l4_protocol;
    }

    public void setCommon_l4_protocol(String common_l4_protocol) {
        this.common_l4_protocol = common_l4_protocol;
    }

    public SessionRecordLog() {
    }

    public String getCommon_l7_protocol() {
        return common_l7_protocol;
    }

    public void setCommon_l7_protocol(String common_l7_protocol) {
        this.common_l7_protocol = common_l7_protocol;
    }
//URL
    //ssl --配置描述信息 透传

    public long getCommon_recv_time() {
        return common_recv_time;
    }

    public void setCommon_recv_time(long common_recv_time) {
        this.common_recv_time = common_recv_time;
    }

    public String getCommon_client_location() {
        return common_client_location;
    }

    public void setCommon_client_location(String common_client_location) {
        this.common_client_location = common_client_location;
    }

    public String getCommon_server_location() {
        return common_server_location;
    }

    public void setCommon_server_location(String common_server_location) {
        this.common_server_location = common_server_location;
    }

    public String getCommon_sled_ip() {
        return common_sled_ip;
    }

    public void setCommon_sled_ip(String common_sled_ip) {
        this.common_sled_ip = common_sled_ip;
    }

    public String getCommon_client_ip() {
        return common_client_ip;
    }

    public void setCommon_client_ip(String common_client_ip) {
        this.common_client_ip = common_client_ip;
    }

    public String getCommon_server_ip() {
        return common_server_ip;
    }

    public void setCommon_server_ip(String common_server_ip) {
        this.common_server_ip = common_server_ip;
    }

    public int getCommon_client_port() {
        return common_client_port;
    }

    public void setCommon_client_port(int common_client_port) {
        this.common_client_port = common_client_port;
    }

    public int getCommon_server_port() {
        return common_server_port;
    }

    public void setCommon_server_port(int common_server_port) {
        this.common_server_port = common_server_port;
    }

    public String getCommon_schema_type() {
        return common_schema_type;
    }

    public void setCommon_schema_type(String common_schema_type) {
        this.common_schema_type = common_schema_type;
    }

    public String getCommon_isp() {
        return common_isp;
    }

    public void setCommon_isp(String common_isp) {
        this.common_isp = common_isp;
    }

    public int getCommon_address_type() {
        return common_address_type;
    }

    public void setCommon_address_type(int common_address_type) {
        this.common_address_type = common_address_type;
    }

    public String getCommon_app_label() {
        return common_app_label;
    }

    public void setCommon_app_label(String common_app_label) {
        this.common_app_label = common_app_label;
    }

    public String getCommon_app_id() {
        return common_app_id;
    }

    public void setCommon_app_id(String common_app_id) {
        this.common_app_id = common_app_id;
    }

    public int getCommon_c2s_pkt_num() {
        return common_c2s_pkt_num;
    }

    public void setCommon_c2s_pkt_num(int common_c2s_pkt_num) {
        this.common_c2s_pkt_num = common_c2s_pkt_num;
    }

    public long getCommon_c2s_byte_num() {
        return common_c2s_byte_num;
    }

    public void setCommon_c2s_byte_num(long common_c2s_byte_num) {
        this.common_c2s_byte_num = common_c2s_byte_num;
    }

    public int getCommon_s2c_pkt_num() {
        return common_s2c_pkt_num;
    }

    public void setCommon_s2c_pkt_num(int common_s2c_pkt_num) {
        this.common_s2c_pkt_num = common_s2c_pkt_num;
    }

    public long getCommon_s2c_byte_num() {
        return common_s2c_byte_num;
    }

    public void setCommon_s2c_byte_num(long common_s2c_byte_num) {
        this.common_s2c_byte_num = common_s2c_byte_num;
    }

    public String getDns_qname() {
        return dns_qname;
    }

    public void setDns_qname(String dns_qname) {
        this.dns_qname = dns_qname;
    }

    public String getDns_rr() {
        return dns_rr;
    }

    public void setDns_rr(String dns_rr) {
        this.dns_rr = dns_rr;
    }

    public String getFtp_url() {
        return ftp_url;
    }

    public void setFtp_url(String ftp_url) {
        this.ftp_url = ftp_url;
    }

    public String getFtp_content() {
        return ftp_content;
    }

    public void setFtp_content(String ftp_content) {
        this.ftp_content = ftp_content;
    }

    public String getHttp_host() {
        return http_host;
    }

    public void setHttp_host(String http_host) {
        this.http_host = http_host;
    }

    public String getHttp_url() {
        return http_url;
    }

    public void setHttp_url(String http_url) {
        this.http_url = http_url;
    }

    public String getHttp_user_agent() {
        return http_user_agent;
    }

    public void setHttp_user_agent(String http_user_agent) {
        this.http_user_agent = http_user_agent;
    }

    public String getHttp_cookie() {
        return http_cookie;
    }

    public void setHttp_cookie(String http_cookie) {
        this.http_cookie = http_cookie;
    }

    public String getHttp_snapshot() {
        return http_snapshot;
    }

    public void setHttp_snapshot(String http_snapshot) {
        this.http_snapshot = http_snapshot;
    }

    public String getSsl_sni() {
        return ssl_sni;
    }

    public void setSsl_sni(String ssl_sni) {
        this.ssl_sni = ssl_sni;
    }

    public long getRecv_time() {
        return recv_time;
    }

    public void setRecv_time(long recv_time) {
        this.recv_time = recv_time;
    }

    public String getMail_from() {
        return mail_from;
    }

    public void setMail_from(String mail_from) {
        this.mail_from = mail_from;
    }

    public String getMail_to() {
        return mail_to;
    }

    public void setMail_to(String mail_to) {
        this.mail_to = mail_to;
    }

    public String getMail_cc() {
        return mail_cc;
    }

    public void setMail_cc(String mail_cc) {
        this.mail_cc = mail_cc;
    }

    public String getMail_subject() {
        return mail_subject;
    }

    public void setMail_subject(String mail_subject) {
        this.mail_subject = mail_subject;
    }

    public String getMail_content() {
        return mail_content;
    }

    public void setMail_content(String mail_content) {
        this.mail_content = mail_content;
    }

    public String getMail_attachment_name() {
        return mail_attachment_name;
    }

    public void setMail_attachment_name(String mail_attachment_name) {
        this.mail_attachment_name = mail_attachment_name;
    }

    public String getMail_attachment_content() {
        return mail_attachment_content;
    }

    public void setMail_attachment_content(String mail_attachment_content) {
        this.mail_attachment_content = mail_attachment_content;
    }

    public String getMail_passwd() {
        return mail_passwd;
    }

    public void setMail_passwd(String mail_passwd) {
        this.mail_passwd = mail_passwd;
    }

    public String getMail_protocol_type() {
        return mail_protocol_type;
    }

    public void setMail_protocol_type(String mail_protocol_type) {
        this.mail_protocol_type = mail_protocol_type;
    }

    public String getMail_snapshot() {
        return mail_snapshot;
    }

    public void setMail_snapshot(String mail_snapshot) {
        this.mail_snapshot = mail_snapshot;
    }
    //应用类
    //邮件类---Location
    //邮件类透传

}
