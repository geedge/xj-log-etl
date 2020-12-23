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
   *bolt设置
   */
   public static final int FLOODING_TOPOLOGY_TICK_TUPLE_FREQ_SECS = StreamAggregateConfigurations.getIntProperty(0,"FLOODING.TOPOLOGY.TICK.TUPLE.FREQ.SECS");
   public static final int FLOODING_BATCH_INTER_NUM = StreamAggregateConfigurations.getIntProperty(0,"FLOODING.BATCH.INTER.NUM");


   /**
   * Flooding-spout-field
   */
   public static final String FLOODING_SPOUT_FIELD = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SPOUT.FIELD");


    /**
     * Flooding-bolt-Streamid
     */

    public static final String FLOODING_DNS_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.DNS.STREAMID");
    public static final String FLOODING_HTTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.HTTP.STREAMID");
    public static final String FLOODING_IP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.IP.STREAMID");
    public static final String FLOODING_MAIL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.MAIL.STREAMID");
    public static final String FLOODING_SSL_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.SSL.STREAMID");
    public static final String FLOODING_VPN_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.VPN.STREAMID");
    public static final String FLOODING_JSTX_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.JSTX.STREAMID");
    public static final String FLOODING_FTP_STREAMID = StreamAggregateConfigurations.getStringProperty(0, "FLOODING.FTP.STREAMID");






}