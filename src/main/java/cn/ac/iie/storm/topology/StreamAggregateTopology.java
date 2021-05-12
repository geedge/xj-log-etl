package cn.ac.iie.storm.topology;

import cn.ac.iie.storm.bolt.conn.*;


import cn.ac.iie.storm.bolt.gk.*;
import cn.ac.iie.storm.bolt.index.ConnCountBolt;
import cn.ac.iie.storm.bolt.index.GkCountBolt;
import cn.ac.iie.storm.bolt.index.JCCountBolt;
import cn.ac.iie.storm.bolt.jc.*;
import cn.ac.iie.storm.spout.FloodingKafkaSpout;
import cn.ac.iie.storm.spout.SafeKafkaSpout;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.topology.TopologyBuilder;




public class StreamAggregateTopology {

 private static Logger logger = Logger.getLogger(StreamAggregateTopology.class);
    private final String topologyName;

    private final Config topologyConfig;
    private TopologyBuilder topologyBuilder;

    private StreamAggregateTopology() {
        this(StreamAggregateTopology.class.getSimpleName());
    }

    private StreamAggregateTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologConfig();
    }

    /**
     * 测试配置
     * conf.setTopologyWorkerMaxHeapSize(6144);
     * conf.put(Config.WORKER_CHILDOPTS, "-Xmx4G -Xms2G");
     */
    private Config createTopologConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(60);
        conf.setMaxSpoutPending(StreamAggregateConfig.MAX_SPOUT_PARALLELISM);
        conf.setNumAckers(StreamAggregateConfig.TOPOLOGY_NUM_ACKS);
        return conf;
    }
    private void buildTopology() {

        topologyBuilder = new TopologyBuilder();


        //设置拓扑关系(Spout)

        //设置拓扑关系(Bolt) --切分行    ---分发bolt
        switch (StreamAggregateConfig.TOPOLOGY_LOGO) {
            case "conn":
                topologyBuilder.setSpout(StreamAggregateConfig.KAFKA_SPOUT, new FloodingKafkaSpout(), StreamAggregateConfig.SPOUT_PARALLELISM);

                topologyBuilder.setBolt(StreamAggregateConfig.CONN_PARSE_COMPONENTID, new ConnParseBolt(), StreamAggregateConfig.PARSE_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.KAFKA_SPOUT);

                topologyBuilder.setBolt("CONN-SEND", new ConnSendBolt(),StreamAggregateConfig.SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_DNS_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_VPN_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_MAIL_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_JSTX_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_FTP_STREAMID);

                topologyBuilder.setBolt("CONN-IP-SEND",new ConnIPSendBolt(),StreamAggregateConfig.IP_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_IP_STREAMID);

                topologyBuilder.setBolt("CONN-HTTP-SEND",new ConnHTTPSendBolt(),StreamAggregateConfig.HTTP_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_HTTP_STREAMID);

                topologyBuilder.setBolt("CONN-SSL-SEND",new ConnSSLSendBolt(),StreamAggregateConfig.SSL_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_SSL_STREAMID);

                topologyBuilder.setBolt("CONN-UNSTRUCTURED-SEND",new ConnUnstructuredSendBolt(),StreamAggregateConfig.UNSTRUCTURED_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,"conn-unstructured");

                topologyBuilder.setBolt("CONN-COUNT", new ConnCountBolt(),StreamAggregateConfig.COUNT_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_YBInputSize_STREAMID)
                        .shuffleGrouping("CONN-SEND")
                        .shuffleGrouping("CONN-IP-SEND")
                        .shuffleGrouping("CONN-HTTP-SEND")
                        .shuffleGrouping("CONN-SSL-SEND");
                break;
            case "gk":
                topologyBuilder.setSpout(StreamAggregateConfig.KAFKA_SPOUT, new SafeKafkaSpout(), StreamAggregateConfig.SPOUT_PARALLELISM);

                topologyBuilder.setBolt("GK-PARSE",new GkParseBolt(),StreamAggregateConfig.PARSE_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.KAFKA_SPOUT);
                topologyBuilder.setBolt(("GK-SEND"),new GkSendBolt(),StreamAggregateConfig.SEND_PARALLELISM)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_DNS_STREAMID)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_FTP_STREAMID)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_VPN_STREAMID)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_MAIL_STREAMID)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_WYGJC_STREAMID)
                            .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_JSTX_STREAMID);
                topologyBuilder.setBolt(("GK-HTTP-SEND"),new GKHTTPSendBolt(),StreamAggregateConfig.HTTP_SEND_PARALLELISM)
                    .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_HTTP_STREAMID);
                topologyBuilder.setBolt(("GK-IP-SEND"),new GKIPSendBolt(),StreamAggregateConfig.IP_SEND_PARALLELISM)
                           .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_IP_STREAMID);
                topologyBuilder.setBolt(("GK-SSL-SEND"),new GKSSLSendBolt(),StreamAggregateConfig.SSL_SEND_PARALLELISM)
                           .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_SSL_STREAMID);
                topologyBuilder.setBolt("GK-UNSTRUCTURED-SEND",new GkUnstructuredSendBolt(),StreamAggregateConfig.UNSTRUCTURED_SEND_PARALLELISM)
                        .shuffleGrouping("GK-PARSE","gk-unstructured");
                topologyBuilder.setBolt("GK-COUNT", new GkCountBolt(),StreamAggregateConfig.COUNT_SEND_PARALLELISM)
                        .shuffleGrouping("GK-PARSE",StreamAggregateConfig.GK_COUNT_STREAMID)
                        .shuffleGrouping("GK-HTTP-SEND")
                        .shuffleGrouping("GK-IP-SEND")
                        .shuffleGrouping("GK-SSL-SEND")
                        .shuffleGrouping("GK-SEND");
            break;
            case "jc":
                topologyBuilder.setSpout(StreamAggregateConfig.KAFKA_SPOUT, new SafeKafkaSpout(), StreamAggregateConfig.SPOUT_PARALLELISM);

                topologyBuilder.setBolt("JC-PARSE",new JCParseBolt(),StreamAggregateConfig.PARSE_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.KAFKA_SPOUT);
                topologyBuilder.setBolt("JC-SEND", new JCSendBolt(),StreamAggregateConfig.SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_DNS_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_VPN_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_MAIL_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_JSTX_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_PIC_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_MEDIA_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_WYGJC_STREAMID)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_FTP_STREAMID);
                topologyBuilder.setBolt("JC-IP-SEND",new JCIPSendBolt(),StreamAggregateConfig.IP_SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_IP_STREAMID);
                topologyBuilder.setBolt("JC-HTTP-SEND",new JCHTTPSendBolt(),StreamAggregateConfig.HTTP_SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_HTTP_STREAMID);
                topologyBuilder.setBolt("JC-SSL-SEND",new JCSSLSendBolt(),StreamAggregateConfig.SSL_SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_SSL_STREAMID);
                topologyBuilder.setBolt("JC-UNSTRUCTURED-SEND",new JcUnstructuredSendBolt(),StreamAggregateConfig.UNSTRUCTURED_SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE","jc-unstructured");
                topologyBuilder.setBolt("JC-COUNT", new JCCountBolt(),StreamAggregateConfig.COUNT_SEND_PARALLELISM)
                        .shuffleGrouping("JC-PARSE",StreamAggregateConfig.JC_COUNT_STREAMID)
                        .shuffleGrouping("JC-SEND")
                        .shuffleGrouping("JC-IP-SEND")
                        .shuffleGrouping("JC-HTTP-SEND")
                        .shuffleGrouping("JC-SSL-SEND");
                break;
            default:
                break;
        }
    }
    private void runLocally() throws InterruptedException {
        StormRunner.runTopologyLocally(topologyBuilder, "localtest", topologyConfig, 6000);
    }

    private void runRemotely() throws AlreadyAliveException, InvalidTopologyException, AuthorizationException {
       topologyConfig.setNumWorkers(StreamAggregateConfig.TOPOLOGY_WORKERS);
        //设置过高会导致很多问题，如心跳线程饿死、吞吐量大幅下跌
        topologyConfig.put(Config.TOPOLOGY_EXECUTOR_RECEIVE_BUFFER_SIZE, 8);
        StormRunner.runTopologyRemotely(topologyBuilder,topologyName, topologyConfig);
    }

    public static void main(String[] args) throws Exception {
        StreamAggregateTopology csst = null;
        boolean runLocally = true;
        String parameter = "remote";
        int size = 2;
        if (args.length >= size && parameter.equalsIgnoreCase(args[0])) {
            runLocally = false;
            csst = new StreamAggregateTopology(args[1]);
        } else {
            csst = new StreamAggregateTopology();
        }
        csst.buildTopology();
        if (runLocally) {
            logger.info("执行本地模式...");
            csst.runLocally();
        } else {
            logger.info("执行远程部署模式...");
            csst.runRemotely();
        }
    }
}
