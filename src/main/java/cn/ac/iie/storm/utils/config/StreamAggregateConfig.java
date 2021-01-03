package cn.ac.iie.storm.utils.config;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    public static final String FORMAT_SPLITTER = ",";
    /**
     * System
     */
    public static final Integer SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"spout.parallelism");
    public static final Integer TOPOLOGY_NUM_ACKS = StreamAggregateConfigurations.getIntProperty(0, "topology.num.acks");
    public static final Integer TOPOLOGY_SPOUT_SLEEP_TIME = StreamAggregateConfigurations.getIntProperty(0, "topology.spout.sleep.time");
    public static final Integer TOPOLOGY_WORKERS = StreamAggregateConfigurations.getIntProperty(0, "topology.workers");


    /**
     * kafka
     */
    public static final String BOOTSTRAP_SERVERS = StreamAggregateConfigurations.getStringProperty(0, "bootstrap.servers");
    public static final String GROUP_ID = StreamAggregateConfigurations.getStringProperty(0, "group.id");
    public static final String GROUP_SAFE_ID = StreamAggregateConfigurations.getStringProperty(0, "group.safe.id");
    public static final String KAFKA_FLOODING_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.flooding.topic");
    public static final String KAFKA_SAFE_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.safe.topic");
    public static final String AUTO_OFFSET_RESET = StreamAggregateConfigurations.getStringProperty(0, "auto.offset.reset");




    //Flooding-bolt设置
    public static final int FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"flooding.topology.tick.tuple.freq.secs");  //定时
    //分批
    //Flooding-spout-field
    public static final String FLOODING_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.spout.field");
    //Flooding-ComponentId-spout
    public static final String FLOODING_KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "flooding.kafka.spout");
    //#flooding-bolt-parallelism_hint
    public static final int FLOODING_PARSE_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.parse.parallelism");
    public static final int FLOODING_DNS_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.dns.parallelism");
    public static final int FLOODING_FTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.ftp.parallelism");
    public static final int FLOODING_HTTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.http.parallelism");
    public static final int FLOODING_IP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.ip.parallelism");
    public static final int FLOODING_JSTX_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.jstx.parallelism");
    public static final int FLOODING_MAIL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.mail.parallelism");
    public static final int FLOODING_SSL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.ssl.parallelism");
    public static final int FLOODING_VPN_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"flooding.vpn.parallelism");


    //Flooding-ComponentId-bolt
    public static final String FLOODING_PARSE_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.parse.componentid");
    public static final String FLOODING_DNS_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.dns.componentid");
    public static final String FLOODING_FTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ftp.componentid");
    public static final String FLOODING_HTTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.http.componentid");
    public static final String FLOODING_IP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ip.componentid");
    public static final String FLOODING_JSTX_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.jstx.componentid");
    public static final String FLOODING_MAIL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.mail.componentid");
    public static final String FLOODING_SSL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ssl.componentid");
    public static final String FLOODING_VPN_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "flooding.vpn.componentid");
    //Flooding-rzzb-schema
    public static final String DNS_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.rzzb.schema");
    public static final String FTP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ftp.rzzb.schema");
    public static final String HTTP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.rzzb.schema");
    public static final String IP_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.rzzb.schema");
    public static final String JSTX_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "jstx.rzzb.schema");
    public static final String MAIL_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "mail.rzzb.schema");
    public static final String SSL_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.rzzb.schema");
    public static final String VPN_RZZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "vpn.rzzb.schema");
     //Flooding-bolt-Streamid
    public static final String FLOODING_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.dns.streamid");
    public static final String FLOODING_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ftp.streamid");
    public static final String FLOODING_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.http.streamid");
    public static final String FLOODING_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ip.streamid");
    public static final String FLOODING_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.jstx.streamid");
    public static final String FLOODING_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.mail.streamid");
    public static final String FLOODING_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.ssl.streamid");
    public static final String FLOODING_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "flooding.vpn.streamid");

    public static final String FLOODING_YBInputSize_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "floding.ybinputsize.componentid");
     //Flooding-Fields-id
    public static final String FLOODING_DNSCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.dnscount.field");
    public static final String FLOODING_DNSLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.dnsloadfail.field");
    public static final String FLOODING_DNSDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.dnsdatasize.field");

    public static final String FLOODING_FTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ftpcount.field");
    public static final String FLOODING_FTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ftploadfail.field");
    public static final String FLOODING_FTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ftpdatasize.field");

    public static final String FLOODING_HTTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.httpcount.field");
    public static final String FLOODING_HTTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.httploadfail.field");
    public static final String FLOODING_HTTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.httpdatasize.field");

    public static final String FLOODING_IPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ipcount.field");
    public static final String FLOODING_IPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.iploadfail.field");
    public static final String FLOODING_IPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ipdatasize.field");


    public static final String FLOODING_JSTXCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.jstxcount.field");
    public static final String FLOODING_JSTXLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.jstxloadfail.field");
    public static final String FLOODING_JSTXDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.jstxdatasize.field");

    public static final String FLOODING_MAILCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.mailcount.field");
    public static final String FLOODING_MAILLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.mailloadfail.field");
    public static final String FLOODING_MAILDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.maildatasize.field");

    public static final String FLOODING_SSLCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.sslcount.field");
    public static final String FLOODING_SSLLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.sslloadfail.field");
    public static final String FLOODING_SSLDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.ssldatasize.field");

    public static final String FLOODING_VPNCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.vpncount.field");
    public static final String FLOODING_VPNLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.vpnloadfail.field");
    public static final String FLOODING_VPNDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "flooding.vpndatasize.field");
    //Flooding-http-post-url
    public static final String FLOODING_HTTP_POST_URL = StreamAggregateConfigurations.getStringProperty(0, "flooding.http.post.url");
    //flooding.http.post.socketTimeOut
    public static final Integer FLOODING_HTTP_POST_SOCKETTIMEOUT = StreamAggregateConfigurations.getIntProperty(0,"flooding.http.post.socketTimeOut");

    /**
     * safa
     */

    //Flooding-spout-field
    public static final String SAFE_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "safe.spout.field");
    //Flooding-ComponentId-spout
    public static final String SAFE_KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "safe.kafka.spout");

     //gk-source-streamid
    public static final String GK_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ip.streamid");
    public static final String GK_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.dns.streamid");
    public static final String GK_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.http.streamid");
    public static final String GK_TDZS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.tdzs.streamid");
    public static final String GK_WYGJZ_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjz.streamid");
    public static final String GK_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.mail.streamid");
    public static final String GK_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ftp.streamid");
    public static final String GK_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.vpn.streamid");
    public static final String GK_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.jstx.streamid");

    //jc-source-streamid
    public static final String JC_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.ip.streamid");
    public static final String JC_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.dns.streamid");
    public static final String JC_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.http.streamid");
    public static final String JC_TDZS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.tdzs.streamid");
    public static final String JC_WYGJZ_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjz.streamid");
    public static final String JC_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.mail.streamid");
    public static final String JC_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.ftp.streamid");
    public static final String JC_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.vpn.streamid");
    public static final String JC_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.jstx.streamid");
    public static final String JC_PIC_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.pic.streamid");
    public static final String JC_MEDIA_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "jc.media.streamid");




    //count出指标的时间间隔(单位为秒，指标是以分钟为单位最好为60的倍数)
    public static final Integer COUNT_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"count.topology.tick.tuple.freq.secs");

    //PROMETHEUS-HTTP-PORT
    public static final String  PROMETHEUS_PUSHGATEWAY_ADDRESS = StreamAggregateConfigurations.getStringProperty(0, "prometheus.pushgateway.address");


}