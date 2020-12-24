package cn.ac.iie.storm.utils.config;


/**
 * @author Administrator
 */
public class StreamAggregateConfig {

    public static final String FORMAT_SPLITTER = ",";
    /**
     * System
     */
   public static final Integer SPOUT_PARALLELISM = StreamAggregateConfigurations.getIntProperty(0, "spout.parallelism");
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

    public static final String IP_LIBRARY = StreamAggregateConfigurations.getStringProperty(0, "ip.library");
    /**
     * http
     */
    public static final String SCHEMA_HTTP = StreamAggregateConfigurations.getStringProperty(0, "schema.http");
    public static final String APP_ID_HTTP = StreamAggregateConfigurations.getStringProperty(0, "app.id.http");

   /**
   * Flooding---schema
   */


   /**
   *Flooding-bolt设置
   */
   public static final int FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"FLOODING.TOPOLOGY.TICK.TUPLE.FREQ.SECS");  //定时
   public static final int FLOODING_BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"FLOODING.BATCH.INTER.NUM");   //分批

   /**
   * Flooding-spout-field
   */
   public static final String FLOODING_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SPOUT.FIELD");

    /**
     * #Flooding-ComponentId-spout
     */
    public static final String FLOODING_KAFKA_SPOUT = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.KAFKA.SPOUT");

    /**
     * #Flooding-ComponentId-bolt
     */

    public static final String FLOODING_PARSE_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLODING.PARSE.COMPONENTID");

    public static final String FLOODING_DNS_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.DNS.COMPONENTID");
    public static final String FLOODING_FTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.FTP.COMPONENTID");
    public static final String FLOODING_HTTP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.HTTP.COMPONENTID");
    public static final String FLOODING_IP_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.IP.COMPONENTID");
    public static final String FLOODING_JSTX_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.JSTX.COMPONENTID");
    public static final String FLOODING_MAIL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.MAIL.COMPONENTID");
    public static final String FLOODING_SSL_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SSL.COMPONENTID");
    public static final String FLOODING_VPN_COMPONENTID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.VPN.COMPONENTID");




    /**
     * Flooding-bolt-Streamid
     */
    public static final String FLOODING_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.DNS.STREAMID");
    public static final String FLOODING_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.FTP.STREAMID");
    public static final String FLOODING_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.HTTP.STREAMID");
    public static final String FLOODING_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.IP.STREAMID");
    public static final String FLOODING_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.JSTX.STREAMID");
    public static final String FLOODING_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.MAIL.STREAMID");
    public static final String FLOODING_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SSL.STREAMID");
    public static final String FLOODING_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.VPN.STREAMID");


    /**
     * #Flooding-Fields-id
     */
    public static final String FLOODING_DNSCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.DNSCOUNT.FIELD");
    public static final String FLOODING_DNSLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.DNSLOADFAIL.FIELD");

    public static final String FLOODING_FTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.FTPCOUNT.FIELD");
    public static final String FLOODING_FTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.FTPLOADFAIL.FIELD");

    public static final String FLOODING_HTTPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.HTTPCOUNT.FIELD");
    public static final String FLOODING_HTTPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.HTTPLOADFAIL.FIELD");

    public static final String FLOODING_IPCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.IPCOUNT.FIELD");
    public static final String FLOODING_IPLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.IPLOADFAIL.FIELD");

    public static final String FLOODING_JSTXCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.JSTXCOUNT.FIELD");
    public static final String FLOODING_JSTXLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.JSTXLOADFAIL.FIELD");

    public static final String FLOODING_MAILCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.MAILCOUNT.FIELD");
    public static final String FLOODING_MAILLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.MAILLOADFAIL.FIELD");

    public static final String FLOODING_SSLCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SSLCOUNT.FIELD");
    public static final String FLOODING_SSLLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SSLLOADFAIL.FIELD");

    public static final String FLOODING_VPNCOUNT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.VPNCOUNT.FIELD");
    public static final String FLOODING_VPNLOADFAIL_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.VPNLOADFAIL.FIELD");






}