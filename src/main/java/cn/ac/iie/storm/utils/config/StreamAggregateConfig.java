package cn.ac.iie.storm.utils.config;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    /**
     * System
     */
    public static final Integer SAFE_SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"safe.spout.parallelism");
    public static final Integer FLOODING_SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0,"flooding.spout.parallelism");
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
    public static final String KAFKA_FLOODING_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.flooding.topic");
    public static final String KAFKA_SAFE_TOPIC = StreamAggregateConfigurations.getStringProperty(0, "kafka.safe.topic");
    public static final String AUTO_OFFSET_RESET = StreamAggregateConfigurations.getStringProperty(0, "auto.offset.reset");
    /**
     * flooding
     */
    //Flooding-bolt设置
    public static final int FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"flooding.topology.tick.tuple.freq.secs");  //定时
    //分批
    public static final int FLOODING_BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"flooding.batch.inter.num");

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

    public static final String FLOODING_YBInputSize_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "floding.ybinputsize.streamid");
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


    public static final String SAFE_INPUTSIZE_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "safe.inputsize.streamid");

    //gk-source-streamid
    public static final String GK_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ip.streamid");
    public static final String GK_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.dns.streamid");
    public static final String GK_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.http.streamid");
    public static final String GK_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ssl.streamid");
    public static final String GK_WYGJC_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjc.streamid");
    public static final String GK_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.mail.streamid");
    public static final String GK_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.ftp.streamid");
    public static final String GK_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.vpn.streamid");
    public static final String GK_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "gk.jstx.streamid");
    //gk-schema
    public static final String IP_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ip.gkrz.schema");
    public static final String DNS_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "dns.gkzb.schema");
    public static final String HTTP_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "http.gkzb.schema");
    public static final String SSL_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ssl.gkzb.schema");
    public static final String WYGJC_GKRZ_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "wygjc.gjcrz.schema");
    public static final String MAIL_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "mail.gkzb.schema");
    public static final String FTP_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "ftp.gkzb.schema");
    public static final String VPN_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "vpn.gkzb.schema");
    public static final String JSTX_GKZB_SCHEMA = StreamAggregateConfigurations.getStringProperty(0, "jstx.gkzb.schema");

    //gk-field-id
    public static final String GK_DNSCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.dnscount.field");
    public static final String GK_DNSLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.dnsloadfail.field");
    public static final String GK_DNSDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.dnsdatasize.field");

    public static final String GK_FTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ftpcount.field");
    public static final String GK_FTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ftploadfail.field");
    public static final String GK_FTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ftpdatasize.field");

    public static final String GK_HTTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.httpcount.field");
    public static final String GK_HTTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.httploadfail.field");
    public static final String GK_HTTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.httpdatasize.field");

    public static final String GK_IPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ipcount.field");
    public static final String GK_IPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.iploadfail.field");
    public static final String GK_IPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ipdatasize.field");


    public static final String GK_JSTXCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.jstxcount.field");
    public static final String GK_JSTXLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.jstxloadfail.field");
    public static final String GK_JSTXDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.jstxdatasize.field");

    public static final String GK_MAILCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.mailcount.field");
    public static final String GK_MAILLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.mailloadfail.field");
    public static final String GK_MAILDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.maildatasize.field");

    public static final String GK_SSLCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.sslcount.field");
    public static final String GK_SSLLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.sslloadfail.field");
    public static final String GK_SSLDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.ssldatasize.field");

    public static final String GK_VPNCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.vpncount.field");
    public static final String GK_VPNLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.vpnloadfail.field");
    public static final String GK_VPNDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.vpndatasize.field");

    public static final String GK_WYGJCCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjccount.field");
    public static final String GK_WYGJCLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjcloadfail.field");
    public static final String GK_WYGJCDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjcdatasize.field");


    //#gk-bolt-parallelism_hint
    public static final int GK_DNS_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.dns.parallelism");
    public static final int GK_FTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.ftp.parallelism");
    public static final int GK_HTTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.http.parallelism");
    public static final int GK_IP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.ip.parallelism");
    public static final int GK_JSTX_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.jstx.parallelism");
    public static final int GK_MAIL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.mail.parallelism");
    public static final int GK_SSL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.ssl.parallelism");
    public static final int GK_VPN_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.vpn.parallelism");
    public static final int GK_WYGJC_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"gk.wygjc.parallelism");

    //gk/jc-ComponentId-bolt
    public static final String SAFE_PARSE_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "safe.parse.componentid");


    public static final String GK_DNS_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.dns.componentid");
    public static final String GK_FTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.ftp.componentid");
    public static final String GK_HTTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.http.componentid");
    public static final String GK_IP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.ip.componentid");
    public static final String GK_JSTX_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.jstx.componentid");
    public static final String GK_MAIL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.mail.componentid");
    public static final String GK_SSL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.ssl.componentid");
    public static final String GK_VPN_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.vpn.componentid");
    public static final String GK_WYGJC_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "gk.wygjc.componentid");

    public static final String JC_DNS_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.dns.componentid");
    public static final String JC_FTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.ftp.componentid");
    public static final String JC_HTTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.http.componentid");
    public static final String JC_IP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.ip.componentid");
    public static final String JC_JSTX_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.jstx.componentid");
    public static final String JC_MAIL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.mail.componentid");
    public static final String JC_SSL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.ssl.componentid");
    public static final String JC_VPN_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.vpn.componentid");
    public static final String JC_WYGJC_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjc.componentid");
    public static final String JC_PIC_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.pic.componentid");
    public static final String JC_MEDIA_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "jc.media.componentid");

    //jc-source-streamid
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

    //jc-schema
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


    //jc-field-id
    public static final String JC_DNSCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.dnscount.field");
    public static final String JC_DNSLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.dnsloadfail.field");
    public static final String JC_DNSDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.dnsdatasize.field");

    public static final String JC_FTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ftpcount.field");
    public static final String JC_FTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ftploadfail.field");
    public static final String JC_FTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ftpdatasize.field");

    public static final String JC_HTTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.httpcount.field");
    public static final String JC_HTTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.httploadfail.field");
    public static final String JC_HTTPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.httpdatasize.field");

    public static final String JC_IPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ipcount.field");
    public static final String JC_IPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.iploadfail.field");
    public static final String JC_IPDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ipdatasize.field");


    public static final String JC_JSTXCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.jstxcount.field");
    public static final String JC_JSTXLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.jstxloadfail.field");
    public static final String JC_JSTXDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.jstxdatasize.field");

    public static final String JC_MAILCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.mailcount.field");
    public static final String JC_MAILLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.mailloadfail.field");
    public static final String JC_MAILDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.maildatasize.field");

    public static final String JC_SSLCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.sslcount.field");
    public static final String JC_SSLLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.sslloadfail.field");
    public static final String JC_SSLDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.ssldatasize.field");

    public static final String JC_VPNCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.vpncount.field");
    public static final String JC_VPNLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.vpnloadfail.field");
    public static final String JC_VPNDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.vpndatasize.field");

    public static final String JC_WYGJCCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjccount.field");
    public static final String JC_WYGJCLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjcloadfail.field");
    public static final String JC_WYGJCDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.wygjcdatasize.field");

    public static final String JC_MEDIACOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.mediacount.field");
    public static final String JC_MEDIALOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.medialoadfail.field");
    public static final String JC_MEDIADATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.mediadatasize.field");

    public static final String JC_PICCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.piccount.field");
    public static final String JC_PICLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.picloadfail.field");
    public static final String JC_PICDATASIZE_FIELD = StreamAggregateConfigurations.getStringProperty(0, "jc.picdatasize.field");


    //#jc-bolt-parallelism_hint
    public static final int JC_DNS_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.dns.parallelism");
    public static final int JC_FTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.ftp.parallelism");
    public static final int JC_HTTP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.http.parallelism");
    public static final int JC_IP_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.ip.parallelism");
    public static final int JC_JSTX_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.jstx.parallelism");
    public static final int JC_MAIL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.mail.parallelism");
    public static final int JC_SSL_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.ssl.parallelism");
    public static final int JC_VPN_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.vpn.parallelism");
    public static final int JC_WYGJC_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.wygjc.parallelism");
    public static final int JC_PIC_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.pic.parallelism");
    public static final int JC_MEDIA_PARALLELISM= StreamAggregateConfigurations.getIntProperty(0,"jc.media.parallelism");


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





}