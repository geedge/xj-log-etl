package cn.ac.iie.storm.utils.config;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {



    public static final String TOPOLOGY_LOGO = StreamAggregateConfigurations.getStringProperty(0, "topology.logo");


    /**
     * System
     */
    public static final Integer SAFE_SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"safe.spout.parallelism");
    public static final Integer CONN_SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.spout.parallelism");
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
    public static final String GROUP_SAFE_ID = StreamAggregateConfigurations.getStringProperty(0, "group.safe.id");
    public static final String KAFKA_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.topic");
    public static final String KAFKA_SAFE_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.safe.topic");
    public static final String AUTO_OFFSET_RESET = StreamAggregateConfigurations.getStringProperty(0, "auto.offset.reset");
    /**
     * flooding
     */
    //Flooding-bolt设置
    public static final int CONN_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"conn.topology.tick.tuple.freq.secs");  //定时
    //分批
    public static final int CONN_BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"conn.batch.inter.num");

    //conn-ComponentId-spout
    public static final String KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "kafka.spout");
    //#conn-bolt-parallelism_hint
    public static final int CONN_PARSE_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.parse.parallelism");
    public static final int CONN_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.send.parallelism");

    public static final int CONN_IP_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.ip.send.parallelism");
    public static final int CONN_HTTP_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.http.send.parallelism");
    public static final int CONN_SSL_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.ssl.send.parallelism");
    public static final int CONN_COUNT_SEND_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"conn.count.send.parallelism");

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

    //Flooding-bolt设置
    //定时
    public static final int SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"safe.topology.tick.tuple.freq.secs");
    //分批
    public static final int SAFE_BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"safe.batch.inter.num");


    //Flooding-spout-field
    public static final String SAFE_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "safe.spout.field");
    //Flooding-ComponentId-spout
    public static final String SAFE_KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "safe.kafka.spout");


    /**
     * http-post
     */
    //Flooding-http-post-url

    //flooding.http.post.socketTimeOut
    public static final Integer FLOODING_HTTP_POST_SOCKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"flooding.http.post.socketTimeOut");

    public static final  String HTTP_HEARDER_FORMAT = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.format");
    public static final  String HTTP_HEARDER_CONECTION = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.conection");

    public static final  String HTTP_HEARD_CONTENT_TYPE = StreamAggregateConfigurations.getStringProperty(0,"http.heard.content.type");

    public static final  String HTTP_HEARDER_USER = StreamAggregateConfigurations.getStringProperty(0,"http.hearder.user");
    public static final  String HTTP_HEARDER_PASSWORD= StreamAggregateConfigurations.getStringProperty(0,"http.hearder.password");


    //flooding.post.topic
    public static final String FLOODING_POST_DNS_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.dns.topic");
    public static final String FLOODING_POST_FTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.ftp.topic");
    public static final String FLOODING_POST_HTTP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.http.topic");
    public static final String FLOODING_POST_IP_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.ip.topic");
    public static final String FLOODING_POST_JSTX_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.jstx.topic");
    public static final String FLOODING_POST_MAIL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.mail.topic");
    public static final String FLOODING_POST_SSL_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.ssl.topic");
    public static final String FLOODING_POST_VPN_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "flooding.post.vpn.topic");



    public static final String  HTTP_POST_FLOODING_IP = StreamAggregateConfigurations.getStringProperty(0, "http.post.flooding.ip");

    public static final String  HTTP_POST_SAFE_IP = StreamAggregateConfigurations.getStringProperty(0, "http.post.safe.ip");


    public static final String  HTTP_POST_PORT = StreamAggregateConfigurations.getStringProperty(0, "http.post.port");

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
    public static final int GK_PARSE_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.parse.parallelism");
    public static final int GK_SEND_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.send.parallelism");


    public static final String IP_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.gkrz.schema");
    public static final String DNS_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.gkzb.schema");
    public static final String HTTP_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.gkzb.schema");
    public static final String SSL_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.gkzb.schema");
    public static final String WYGJC_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "wygjc.gjcrz.schema");
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



}