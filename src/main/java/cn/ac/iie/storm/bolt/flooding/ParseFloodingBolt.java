package cn.ac.iie.storm.bolt.flooding;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.alibaba.fastjson.JSONObject;

import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

import static cn.ac.iie.storm.utils.general.TransFormUtils.getJsonMessage;

public class ParseFloodingBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ParseFloodingBolt.class);
    private SessionRecordLog DNSsource;
    private SessionRecordLog HTTPSource;
    private SessionRecordLog IPSource;
    private SessionRecordLog MAILSource;
    private SessionRecordLog JSTXSource;
    private SessionRecordLog FTPSource;
    private SessionRecordLog SSLSource;
    private SessionRecordLog VPNSource;

    /**
     * 处理数据的逻辑业务
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        //首先获取一条数据
        try {
            String line = tuple.getStringByField(StreamAggregateConfig.FLOODING_SPOUT_FIELD);
            String jsonMessage = getJsonMessage(line);
            SessionRecordLog sessionRecordLog = JSONObject.parseObject(jsonMessage, SessionRecordLog.class);
            String common_schema_type = sessionRecordLog.getCommon_schema_type();

            //按common_schema_type的值对数据进行分类发送
            switch(common_schema_type){
                case "BASE":
                if(sessionRecordLog.getCommon_schema_type()==null){
                    VPNSource=sessionRecordLog;
                }else {
                    sessionRecordLog.setCommon_schema_type("IP类");
                    IPSource=sessionRecordLog;
                }
                    break;
                case "DNS":
                   DNSsource= sessionRecordLog;
                    break;
                case "HTTP":
                    HTTPSource=sessionRecordLog;
                    break;
                case "MAIL":
                    MAILSource=sessionRecordLog;
                    break;
                case "APP":
                    JSTXSource=sessionRecordLog;

                    break;
                case "FTP":
                    FTPSource=sessionRecordLog;

                    break;
                case "SSL":
                    SSLSource=sessionRecordLog;

                    break;
                default:
                    break;
            }
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_VPN_STREAMID,new Values(VPNSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_IP_STREAMID,new Values(IPSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_DNS_STREAMID,new Values(DNSsource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_HTTP_STREAMID,new Values(HTTPSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_MAIL_STREAMID,new Values(MAILSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_JSTX_STREAMID,new Values(JSTXSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_FTP_STREAMID,new Values(FTPSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_SSL_STREAMID,new Values(SSLSource));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }



    @Override
    public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer){
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_VPN_STREAMID,new Fields(StreamAggregateConfig.FLOODING_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_IP_STREAMID,new Fields(StreamAggregateConfig.FLOODING_IP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_DNS_STREAMID,new Fields(StreamAggregateConfig.FLOODING_DNS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_HTTP_STREAMID,new Fields(StreamAggregateConfig.FLOODING_HTTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_MAIL_STREAMID,new Fields(StreamAggregateConfig.FLOODING_MAIL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_JSTX_STREAMID,new Fields(StreamAggregateConfig.FLOODING_JSTX_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_FTP_STREAMID,new Fields(StreamAggregateConfig.FLOODING_FTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_SSL_STREAMID,new Fields(StreamAggregateConfig.FLOODING_SSL_STREAMID));

    }
}
