package cn.ac.iie.storm.topology;

import cn.ac.iie.storm.bolt.flooding.ParseFloodingBolt;
import cn.ac.iie.storm.bolt.flooding.*;
import cn.ac.iie.storm.bolt.index.CountBolt;
import cn.ac.iie.storm.bolt.safe.ParseSafeBolt;
import cn.ac.iie.storm.bolt.safe.gk.*;
import cn.ac.iie.storm.bolt.safe.jc.*;
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
        //创建任务的拓扑图
        topologyBuilder = new TopologyBuilder();
        //设置拓扑关系(FloodingSpout)
        topologyBuilder.setSpout(StreamAggregateConfig.FLOODING_KAFKA_SPOUT, new FloodingKafkaSpout(),StreamAggregateConfig.FLOODING_SPOUT_PARALLELISM);
        //设置拓扑关系(Bolt) --切分行    ---分发bolt
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, new ParseFloodingBolt(),StreamAggregateConfig.FLOODING_PARSE_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_KAFKA_SPOUT);
        //分类发送的bolt
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_DNS_COMPONENTID, new FloodingDNSBolt(), StreamAggregateConfig.FLOODING_DNS_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_DNS_STREAMID);  //无数据，还未测试
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_FTP_COMPONENTID, new FloodingFTPBolt(), StreamAggregateConfig.FLOODING_FTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_FTP_STREAMID);  //无数据，还未测试
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID, new FloodingHTTPBolt(), StreamAggregateConfig.FLOODING_HTTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_HTTP_STREAMID);  //测试通过
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_IP_COMPONENTID, new FloodingIPBolt(), StreamAggregateConfig.FLOODING_IP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_IP_STREAMID);  //测试通过
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID, new FloodingJSTXBolt(), StreamAggregateConfig.FLOODING_JSTX_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_JSTX_STREAMID); //无数据，还未测试
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID, new FloodingMAILBolt(), StreamAggregateConfig.FLOODING_MAIL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_MAIL_STREAMID);  //无数据 还未测试
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_SSL_COMPONENTID, new FloodingSSLBolt(), StreamAggregateConfig.FLOODING_SSL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_SSL_STREAMID);
        topologyBuilder.setBolt(StreamAggregateConfig.FLOODING_VPN_COMPONENTID, new FloodingVPNBolt(), StreamAggregateConfig.FLOODING_VPN_PARALLELISM).shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_VPN_STREAMID);    //测试通过


//        //设置拓扑关系(Spout)
//            topologyBuilder.setSpout(StreamAggregateConfig.SAFE_KAFKA_SPOUT,new SafeKafkaSpout(),StreamAggregateConfig.SAFE_SPOUT_PARALLELISM);
//
//        //设置拓扑关系(Bolt) --切分行
//        topologyBuilder.setBolt(StreamAggregateConfig.SAFE_PARSE_COMPONENTID,new ParseSafeBolt()).shuffleGrouping(StreamAggregateConfig.SAFE_KAFKA_SPOUT);
//
//        //gk
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_DNS_COMPONENTID, new GKDNSBolt(), StreamAggregateConfig.GK_DNS_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_DNS_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_FTP_COMPONENTID, new GKFTPBolt(), StreamAggregateConfig.GK_FTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_FTP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_HTTP_COMPONENTID, new GKHTTPBolt(), StreamAggregateConfig.GK_HTTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_HTTP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_IP_COMPONENTID, new GKIPBolt(), StreamAggregateConfig.GK_IP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_IP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_JSTX_COMPONENTID, new GKJSTXBolt(), StreamAggregateConfig.GK_JSTX_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_JSTX_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_MAIL_COMPONENTID, new GKMAILBolt(), StreamAggregateConfig.GK_MAIL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_MAIL_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_SSL_COMPONENTID, new GKSSLBolt(), StreamAggregateConfig.GK_SSL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_SSL_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_VPN_COMPONENTID, new GKVPNBolt(), StreamAggregateConfig.GK_VPN_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_VPN_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.GK_WYGJC_COMPONENTID, new GKVPNBolt(), StreamAggregateConfig.GK_WYGJC_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.GK_WYGJC_STREAMID);
//
//        //jc
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_DNS_COMPONENTID, new JCDNSBolt(), StreamAggregateConfig.JC_DNS_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_DNS_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_FTP_COMPONENTID, new JCFTPBolt(), StreamAggregateConfig.JC_FTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_FTP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_HTTP_COMPONENTID, new JCHTTPBolt(), StreamAggregateConfig.JC_HTTP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_HTTP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_IP_COMPONENTID, new JCIPBolt(), StreamAggregateConfig.JC_IP_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_IP_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_JSTX_COMPONENTID, new JCJSTXBolt(), StreamAggregateConfig.JC_JSTX_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_JSTX_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_MAIL_COMPONENTID, new JCMAILBolt(), StreamAggregateConfig.JC_MAIL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_MAIL_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_SSL_COMPONENTID, new JCSSLBolt(), StreamAggregateConfig.JC_SSL_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_SSL_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_VPN_COMPONENTID, new JCVPNBolt(), StreamAggregateConfig.JC_VPN_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_VPN_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_WYGJC_COMPONENTID, new JCVPNBolt(), StreamAggregateConfig.JC_WYGJC_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_WYGJC_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_PIC_COMPONENTID, new JCVPNBolt(), StreamAggregateConfig.JC_PIC_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_PIC_STREAMID);
//        topologyBuilder.setBolt(StreamAggregateConfig.JC_MEDIA_COMPONENTID, new JCVPNBolt(), StreamAggregateConfig.JC_MEDIA_PARALLELISM).shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID, StreamAggregateConfig.JC_MEDIA_STREAMID);

        // 总数count
        topologyBuilder.setBolt("count", new CountBolt(), 1)
                .shuffleGrouping(StreamAggregateConfig.FLOODING_DNS_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_FTP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_HTTP_COMPONENTID)
                .shuffleGrouping(StreamAggregateConfig.FLOODING_IP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_JSTX_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_MAIL_COMPONENTID)
                .shuffleGrouping(StreamAggregateConfig.FLOODING_SSL_COMPONENTID).shuffleGrouping(StreamAggregateConfig.FLOODING_VPN_COMPONENTID)
                .shuffleGrouping(StreamAggregateConfig.FLOODING_PARSE_COMPONENTID, StreamAggregateConfig.FLOODING_YBInputSize_STREAMID);

//                .shuffleGrouping(StreamAggregateConfig.GK_DNS_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_FTP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_HTTP_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.GK_IP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_JSTX_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_MAIL_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.GK_SSL_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_VPN_COMPONENTID).shuffleGrouping(StreamAggregateConfig.GK_WYGJC_COMPONENTID)
//
//
//                .shuffleGrouping(StreamAggregateConfig.JC_DNS_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_FTP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_HTTP_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.JC_IP_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_JSTX_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_MAIL_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.JC_SSL_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_VPN_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_WYGJC_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.JC_PIC_COMPONENTID).shuffleGrouping(StreamAggregateConfig.JC_MEDIA_COMPONENTID)
//                .shuffleGrouping(StreamAggregateConfig.SAFE_PARSE_COMPONENTID,StreamAggregateConfig.SAFE_INPUTSIZE_STREAMID);
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
