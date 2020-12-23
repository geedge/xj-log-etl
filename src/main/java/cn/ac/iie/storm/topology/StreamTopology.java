package cn.ac.iie.storm.topology;

import cn.ac.iie.storm.bolt.flooding.FloodingHTTPBolt;
import cn.ac.iie.storm.bolt.flooding.ParseFloodingBolt;
import cn.ac.iie.storm.bolt.safe.ParseSafeBolt;
import cn.ac.iie.storm.spout.FloodingKafkaSpout;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;

public class StreamTopology {
    private static Logger logger = Logger.getLogger(StreamTopology.class);
    private final String topologyName;
    private final Config topologyConfig;
    private TopologyBuilder builder;


    private StreamTopology() {
        this(StreamTopology.class.getSimpleName());
    }

    private StreamTopology(String topologyName) {
        this.topologyName = topologyName;
        topologyConfig = createTopologConfig();
    }

    private Config createTopologConfig() {
        Config conf = new Config();
        conf.setDebug(false);
        conf.setMessageTimeoutSecs(60);
        conf.setMaxSpoutPending(StreamAggregateConfig.SPOUT_PARALLELISM);
        conf.setNumAckers(StreamAggregateConfig.TOPOLOGY_NUM_ACKS);
        return conf;
    }


    /**
     * 个人测试
     * @param args
     */
    public static void main(String[] args) {


        //创建任务的拓扑图
        TopologyBuilder topologyBuilder = new TopologyBuilder();


        //设置拓扑关系(FloodingSpout)
        topologyBuilder.setSpout("FloodingKafkaSpout",new FloodingKafkaSpout());

        //设置拓扑关系(Bolt) --切分行    ---分发bolt
        topologyBuilder.setBolt("ParseFloodingBolt",new ParseFloodingBolt()).shuffleGrouping("FloodingKafkaSpout");

        //分类发送的bolt
       // topologyBuilder.setBolt("FloodingVPNBolt",new FloodingVPNBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_VPN_STREAMID);    //测试通过
       // topologyBuilder.setBolt("FloodingDNSBolt",new FloodingDNSBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_DNS_STREAMID);  //无数据，还未测试
       // topologyBuilder.setBolt("FloodingIPBolt",new FloodingIPBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_IP_STREAMID);  //测试通过
         topologyBuilder.setBolt("FloodingHTTPBolt",new FloodingHTTPBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_HTTP_STREAMID);  //测试通过
      //  topologyBuilder.setBolt("FloodingMAILBolt",new FloodingMAILBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_MAIL_STREAMID);  //无数据 还未测试
      //  topologyBuilder.setBolt("FloodingJSTXBolt",new FloodingJSTXBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_JSTX_STREAMID); //无数据，还未测试
    //    topologyBuilder.setBolt("FloodingFTPBolt",new FloodingFTPBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_FTP_STREAMID);  //无数据，还未测试
     //   topologyBuilder.setBolt("FloodingSSLBolt",new FloodingSSLBolt()).shuffleGrouping("ParseFloodingBolt",StreamAggregateConfig.FLOODING_SSL_STREAMID);




        //设置拓扑关系(Spout)
        topologyBuilder.setSpout("SafeKafkaSpout",new FloodingKafkaSpout());

        //设置拓扑关系(Bolt) --切分行
        topologyBuilder.setBolt("ParseSafeBolt",new ParseSafeBolt()).shuffleGrouping("SafeKafkaSpout");

        //总数count
     //   topologyBuilder.setBolt("count",new CountBolt()).shuffleGrouping("FloodingHTTPBolt").shuffleGrouping("FloodingIPBolt");


        //启动Topology
        Config config = new Config();
        //创建一个topology
        StormTopology topology = topologyBuilder.createTopology();
        //本地模式启动集群
        LocalCluster iLocalCluster = new LocalCluster();
        iLocalCluster.submitTopology("KafkaSpout",config,topology);
    }

}
