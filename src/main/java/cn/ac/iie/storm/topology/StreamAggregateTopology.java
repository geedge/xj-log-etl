package cn.ac.iie.storm.topology;

import cn.ac.iie.storm.bolt.flooding.*;


import cn.ac.iie.storm.bolt.gk.GkParseBolt;
import cn.ac.iie.storm.bolt.gk.GkSendBolt;
import cn.ac.iie.storm.bolt.index.ConnCountBolt;
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
                topologyBuilder.setSpout(StreamAggregateConfig.KAFKA_SPOUT, new FloodingKafkaSpout(), StreamAggregateConfig.CONN_SPOUT_PARALLELISM);

                topologyBuilder.setBolt(StreamAggregateConfig.CONN_PARSE_COMPONENTID, new ConnParseBolt(), StreamAggregateConfig.CONN_PARSE_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.KAFKA_SPOUT);

                topologyBuilder.setBolt("conn-send", new ConnSendBolt(),StreamAggregateConfig.CONN_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_DNS_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_VPN_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_MAIL_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_JSTX_STREAMID)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_FTP_STREAMID);

                topologyBuilder.setBolt("conn-ip-send",new ConnIPSendBolt(),StreamAggregateConfig.CONN_IP_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_IP_STREAMID);

                topologyBuilder.setBolt("conn-http-send",new ConnHTTPSendBolt(),StreamAggregateConfig.CONN_HTTP_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_HTTP_STREAMID);

                topologyBuilder.setBolt("conn-ssl-send",new ConnSSLSendBolt(),StreamAggregateConfig.CONN_SSL_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_SSL_STREAMID);

                topologyBuilder.setBolt("count", new ConnCountBolt(),StreamAggregateConfig.CONN_COUNT_SEND_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.CONN_PARSE_COMPONENTID,StreamAggregateConfig.CONN_YBInputSize_STREAMID);
                break;
            case "gk":
                topologyBuilder.setSpout(StreamAggregateConfig.KAFKA_SPOUT, new SafeKafkaSpout(), StreamAggregateConfig.SAFE_SPOUT_PARALLELISM);

                topologyBuilder.setBolt("gk-parse",new GkParseBolt(),StreamAggregateConfig.GK_PARSE_PARALLELISM)
                        .shuffleGrouping(StreamAggregateConfig.KAFKA_SPOUT);

                topologyBuilder.setBolt(("gk-send"),new GkSendBolt(),StreamAggregateConfig.GK_SEND_PARALLELISM)
                        .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_DNS_STREAMID);

            //      .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_IP_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_HTTP_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_VPN_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_FTP_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_SSL_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_MAIL_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_WYGJC_STREAMID)
            //     .shuffleGrouping("gk-parse",StreamAggregateConfig.GK_JSTX_STREAMID)
                break;
            case "jc":
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
