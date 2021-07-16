package cn.ac.iie.storm.utils.config;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {



    public static final String TOPOLOGY_LOGO = StreamAggregateConfigurations.getStringProperty(0, "topology.logo");


    /**
     * System
     */
    public static final Integer SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"spout.parallelism");
    public static final Integer MAX_SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"max.spout.parallelism");
    public static final Integer TOPOLOGY_NUM_ACKS = StreamAggregateConfigurations.getIntProperty(0, "topology.num.acks");
    public static final Integer TOPOLOGY_SPOUT_SLEEP_TIME = StreamAggregateConfigurations.getIntProperty(0, "topology.spout.sleep.time");
    public static final Integer TOPOLOGY_WORKERS = StreamAggregateConfigurations.getIntProperty(0, "topology.workers");


    /**
     * kafka
     */

    public static final Long KAFKA_COMSUMER_POLL = StreamAggregateConfigurations.getLongProperty(0, "kafka.consumer.poll");

    public static final String BOOTSTRAP_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final String GROUP_ID = StreamAggregateConfigurations.getStringProperty(0, "group.id");

    public static final String KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.topic");
    public static final String KAFKA_SAFE_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.safe.topic");
    public static final String AUTO_OFFSET_RESET = StreamAggregateConfigurations.getStringProperty(0, "auto.offset.reset");
    /**
     * flooding
     */
    //Flooding-bolt设置
    public static final int TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"topology.tick.tuple.freq.secs");  //定时
    //分批
    public static final int BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"batch.inter.num");

    //conn-ComponentId-spout
    public static final String KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "kafka.spout");
    //#conn-bolt-parallelism_hint
    public static final int PARSE_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"parse.parallelism");
    public static final int SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"send.parallelism");
    public static final int IP_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"ip.send.parallelism");
    public static final int HTTP_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"http.send.parallelism");
    public static final int SSL_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"ssl.send.parallelism");
    public static final int COUNT_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"count.send.parallelism");
    public static final int UNSTRUCTURED_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"unstructured.send.parallelism");


    //conn-ComponentId-bolt
    public static final String CONN_PARSE_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "conn.parse.componentid");

    public static final String DNS_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.rzzb.schema");
    public static final String FTP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ftp.rzzb.schema");
    public static final String HTTP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.rzzb.schema");
    public static final String IP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.rzzb.schema");
    public static final String JSTX_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "jstx.rzzb.schema");
    public static final String MAIL_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "mail.rzzb.schema");
    public static final String SSL_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.rzzb.schema");
    public static final String VPN_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "vpn.rzzb.schema");
     //conn-bolt-Streamid
    public static final String CONN_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.dns.streamid");
    public static final String CONN_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.ftp.streamid");
    public static final String CONN_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.http.streamid");
    public static final String CONN_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.ip.streamid");
    public static final String CONN_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.jstx.streamid");
    public static final String CONN_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.mail.streamid");
    public static final String CONN_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.ssl.streamid");
    public static final String CONN_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.vpn.streamid");

    public static final String CONN_YBInputSize_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "conn.ybinputsize.streamid");

    /**
     * safa
     */

    //Flooding-spout-field
    public static final String SAFE_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "safe.spout.field");


    /**
     * http-post
     */
    //Flooding-http-post-url

    //flooding.http.post.socketTimeOut
    public static final Integer HTTP_CONNECTIONREQUESTTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"http.connectionRequestTimeout");
    public static final Integer HTTP_CONNECTTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"http.connectTimeout");
    public static final Integer HTTP_SOKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"http.socketTimeout");


    public static final Integer HTTP_SENT_COUNT = StreamAggregateConfigurations.getIntProperty(0,"http.sent.count");
    public static final Integer HTTP_RETRY_COUNT = StreamAggregateConfigurations.getIntProperty(0,"http.retry.count");
    public static final Integer HTTP_POST_SOCKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"http.post.socketTimeOut");

    public static final  String HTTP_HEARDER_FORMAT = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.format");
    public static final  String HTTP_HEARDER_CONECTION = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.conection");

    public static final  String HTTP_HEARD_CONTENT_TYPE = StreamAggregateConfigurations.getStringProperty(0,"http.heard.content.type");

    public static final  String HTTP_HEARDER_USER = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.user");
    public static final  String HTTP_HEARDER_PASSWORD= StreamAggregateConfigurations.getStringProperty(0,"http.hearder.password");


    //flooding.post.topic
    public static final String CONN_POST_DNS_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.dns.topic");
    public static final String CONN_POST_FTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.ftp.topic");
    public static final String CONN_POST_HTTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.http.topic");
    public static final String CONN_POST_IP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.ip.topic");
    public static final String CONN_POST_JSTX_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.jstx.topic");
    public static final String CONN_POST_MAIL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.mail.topic");
    public static final String CONN_POST_SSL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.ssl.topic");
    public static final String CONN_POST_VPN_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "conn.post.vpn.topic");



    public static final String  HTTP_POST_IP = StreamAggregateConfigurations.getStringProperty(0, "http.post.ip");

    public static final String  HTTP_POST_PORT = StreamAggregateConfigurations.getStringProperty(0, "http.post.port");

    public static final String  S3_POST_IP = StreamAggregateConfigurations.getStringProperty(0, "s3.post.ip");

    public static final String  S3_POST_PORT = StreamAggregateConfigurations.getStringProperty(0, "s3.post.port");

    public static final String  HOS_URL = StreamAggregateConfigurations.getStringProperty(0, "hos.url");

    public static final int HOS_POOL_MAXCONNECTIONS = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.maxconnections");
    public static final int HOS_POOL_MAXERRORRETRY = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.maxerrorretry");
    public static final int HOS_POOL_REQUESTTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.requesttimeout");
    public static final int HOS_POOL_CONNECTIONTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.connectiontimeout");
    public static final int HOS_POOL_CONNECTIONTTL = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.connectionttl");
    public static final int HOS_POOL_SOCKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"hos.pool.sockettimeout");



    public static final Integer S3_CONNECTIONREQUESTTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"s3.connectionRequestTimeout");
    public static final Integer S3_CONNECTTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"s3.connectTimeout");
    public static final Integer S3_SOKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"s3.socketTimeout");
    public static final Integer S3_SENT_COUNT = StreamAggregateConfigurations.getIntProperty(0,"s3.sent.count");
    public static final String  S3_HEARDER_USERNAME = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.username");
    public static final String  S3_HEARDER_PASSWORD = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.password");
    public static final String  S3_HEARDER_FILETYPE = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.filetype");
    public static final String  S3_HEARDER_FILETITLE = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.filetitle");
    public static final String  S3_HEARDER_ISFULLTEX = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.isfulltex");
    public static final String  S3_HEARDER_CONTENTTYPE = StreamAggregateConfigurations.getStringProperty(0, "s3.hearder.contenttype");
    /**
     * count
     */
    //count出指标的时间间隔(单位为秒，指标是以分钟为单位最好为60的倍数)
    public static final Integer COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"count.topology.tick.tuple.freq.secs");

    //PROMETHEUS-HTTP-PORT
    public static final String  PROMETHEUS_PUSHGATEWAY_ADDRESS = StreamAggregateConfigurations.getStringProperty(0, "prometheus.pushgateway.address");


    /**
     * gk
     */
    public static final String IP_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.gkrz.schema");
    public static final String DNS_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.gkzb.schema");
    public static final String HTTP_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.gkzb.schema");
    public static final String SSL_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.gkzb.schema");
    public static final String WYGJC_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "wygjc.gkrz.schema");
    public static final String MAIL_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "mail.gkzb.schema");
    public static final String FTP_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ftp.gkzb.schema");
    public static final String VPN_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "vpn.gkzb.schema");
    public static final String JSTX_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "jstx.gkzb.schema");


    public static final String GK_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ip.streamid");
    public static final String GK_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.dns.streamid");
    public static final String GK_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.http.streamid");
    public static final String GK_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ssl.streamid");
    public static final String GK_WYGJC_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjc.streamid");
    public static final String GK_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.mail.streamid");
    public static final String GK_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ftp.streamid");
    public static final String GK_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.vpn.streamid");
    public static final String GK_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.jstx.streamid");

    public static final String GK_COUNT_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.count.streamid");




    public static final String GK_POST_DNS_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.dns.topic");
    public static final String GK_POST_FTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.ftp.topic");
    public static final String GK_POST_HTTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.http.topic");
    public static final String GK_POST_IP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.ip.topic");
    public static final String GK_POST_JSTX_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.jstx.topic");
    public static final String GK_POST_MAIL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.mail.topic");
    public static final String GK_POST_SSL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.ssl.topic");
    public static final String GK_POST_VPN_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.vpn.topic");
    public static final String GK_POST_WYGJC_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "gk.post.wygjc.topic");


    /**
     * jc
     */
    public static final String JC_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.ip.streamid");
    public static final String JC_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.dns.streamid");
    public static final String JC_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.http.streamid");
    public static final String JC_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.ssl.streamid");
    public static final String JC_WYGJC_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjc.streamid");
    public static final String JC_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.mail.streamid");
    public static final String JC_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.ftp.streamid");
    public static final String JC_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.vpn.streamid");
    public static final String JC_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.jstx.streamid");
    public static final String JC_PIC_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.pic.streamid");
    public static final String JC_MEDIA_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.media.streamid");
    public static final String JC_COUNT_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.count.streamid");


    public static final String DNS_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.jczb.schema");
    public static final String FTP_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ftp.jczb.schema");
    public static final String HTTP_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.jczb.schema");
    public static final String IP_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.jczb.schema");
    public static final String JSTX_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "jstx.jczb.schema");
    public static final String MAIL_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "mail.jczb.schema");
    public static final String SSL_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.jczb.schema");
    public static final String VPN_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "vpn.jczb.schema");
    public static final String WYGJZ_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "wygjz.jczb.schema");
    public static final String PIC_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "pic.jczb.schema");
    public static final String MEDIA_JCZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "media.jczb.schema");



    public static final String JC_POST_DNS_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.dns.topic");
    public static final String JC_POST_FTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.ftp.topic");
    public static final String JC_POST_HTTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.http.topic");
    public static final String JC_POST_IP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.ip.topic");
    public static final String JC_POST_JSTX_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.jstx.topic");
    public static final String JC_POST_MAIL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.mail.topic");
    public static final String JC_POST_SSL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.ssl.topic");
    public static final String JC_POST_VPN_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.vpn.topic");
    public static final String JC_POST_WYGJC_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.wygjc.topic");
    public static final String JC_POST_PIC_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.pic.topic");
    public static final String JC_POST_MEDIA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "jc.post.media.topic");


    public static final String T_HTTP_RZZB = StreamAggregateConfigurations.getStringProperty(0, "t.http.rzzb");
    public static final String T_HTTP_JCZB = StreamAggregateConfigurations.getStringProperty(0, "t.http.jczb");
    public static final String T_HTTP_GKZB = StreamAggregateConfigurations.getStringProperty(0, "t.http.gkzb");


    public static final String T_MAIL_RZZB = StreamAggregateConfigurations.getStringProperty(0, "t.mail.rzzb");
    public static final String T_MAIL_JCZB = StreamAggregateConfigurations.getStringProperty(0, "t.mail.jczb");
    public static final String T_MAIL_GKZB = StreamAggregateConfigurations.getStringProperty(0, "t.mail.gkzb");

    public static final String T_WYGJC_JCRZ = StreamAggregateConfigurations.getStringProperty(0, "t.wygjc.jcrz");
    public static final String T_WYGJC_GKRZ = StreamAggregateConfigurations.getStringProperty(0, "t.wygjc.gkrz");

    public static final String T_PIC_JCZB = StreamAggregateConfigurations.getStringProperty(0, "t.pic.jczb");
    public static final String T_MEDIA_JCZB = StreamAggregateConfigurations.getStringProperty(0, "t.media.jczb");



}