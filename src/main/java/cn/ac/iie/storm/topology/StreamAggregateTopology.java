package cn.ac.iie.storm.topology;


import cn.ac.iie.storm.bolt.flooding.*;
import cn.ac.iie.storm.bolt.index.CountBolt;

import cn.ac.iie.storm.spout.FloodingKafkaSpout;
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
        conf.setMaxSpoutPending(StreamAggregateConfig.SPOUT_PARALLELISM);
       conf.setNumAckers(StreamAggregateConfig.TOPOLOGY_NUM_ACKS);
        return conf;
    }
    private void buildTopology() {
        //创建任务的拓扑图
        topologyBuilder = new TopologyBuilder();
        //设置拓扑关系(FloodingSpout)
        topologyBuilder.setSpout(StreamAggregateConfig.FLOODING_KAFKA_SPOUT,new FloodingKafkaSpout());
        //设置拓扑关系(Bolt) --切分行    ---分发bolt
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,new ParseFloodingBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_KAFKA_SPOUT);
        //分类发送的bolt
   topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_DNS_COMPONENTID,new FloodingDNSBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_DNS_STREAMID);  //无数据，还未测试
       topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_FTP_COMPONENTID,new FloodingFTPBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_FTP_STREAMID);  //无数据，还未测试
      topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID,new FloodingHTTPBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_HTTP_STREAMID);  //测试通过
  topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_IP_COMPONENTID,new FloodingIPBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_IP_STREAMID);  //测试通过
      topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID,new FloodingJSTXBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_JSTX_STREAMID); //无数据，还未测试
      topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID,new FloodingMAILBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_MAIL_STREAMID);  //无数据 还未测试
      topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_SSL_COMPONENTID,new FloodingSSLBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_SSL_STREAMID);
       topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_VPN_COMPONENTID,new FloodingVPNBolt()).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_VPN_STREAMID);    //测试通过




//        //设置拓扑关系(Spout)
//        topologyBuilder.setSpout("SafeKafkaSpout",new FloodingKafkaSpout());
//
//        //设置拓扑关系(Bolt) --切分行
//        topologyBuilder.setBolt("ParseSafeBolt",new ParseSafeBolt()).shuffleGrouping("SafeKafkaSpout");



        //总数count
       topologyBuilder.setBolt("count",new CountBolt(),1).shuffleGrouping(StreamAggregateConfig.FLOODING_DNS_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_FTP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID)
               .shuffleGrouping(StreamAggregateConfig.FLOODING_IP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID)
            .shuffleGrouping(StreamAggregateConfig.FLOODING_SSL_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_VPN_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID,StreamAggregateConfig.FLOODING_YBInputSize_STREAMID);



      // topologyBuilder.setBolt("count",new CountBolt(),1).shuffleGrouping(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID);//.shuffleGrouping(StreamAggregateConfig.FLOODING_IP_COMPONENTID);


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
