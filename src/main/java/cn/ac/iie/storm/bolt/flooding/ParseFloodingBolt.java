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
    private static final long serialVersionUID = 7483439472002862539L;

    private String dnsSource;
    private String httpSource;
    private String ipSource;
    private String mailSource;
    private String jstxSource;
    private String ftpSource;
    private String sslSource;
    private String vpnSource;
    private long ybInputSize = 0L;

    /**
     * 处理一般数据的逻辑业务
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
            ybInputSize=line.getBytes().length;
            //按common_schema_type的值对数据进行分类发送
            switch(common_schema_type){
                case "BASE":
                if(sessionRecordLog.getCommon_schema_type()==null){
                    vpnSource=line;
                }else {
                    sessionRecordLog.setCommon_schema_type("IP类");
                    ipSource=line;
                }
                    break;
                case "DNS":
                    dnsSource = line;
                    break;
                case "HTTP":
                    httpSource = line;
                    break;
                case "MAIL":
                    mailSource = line;
                    break;
                case "APP":
                    jstxSource = line;
                    break;
                case "FTP":
                    ftpSource = line;
                    break;
                case "SSL":
                    sslSource = line;
                    break;
                default:
                    break;
            }
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_VPN_STREAMID,new Values(vpnSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_IP_STREAMID,new Values(ipSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_DNS_STREAMID,new Values(dnsSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_HTTP_STREAMID,new Values(httpSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_MAIL_STREAMID,new Values(mailSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_JSTX_STREAMID,new Values(jstxSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_FTP_STREAMID,new Values(ftpSource));
            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_SSL_STREAMID,new Values(sslSource));

            basicOutputCollector.emit(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID,new Values(ybInputSize));
        } catch (Exception e) {
            logger.error("flooding分类分发消息处理异常"+e);
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


        outputFieldsDeclarer.declareStream(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID,new Fields(StreamAggregateConfig.FLOODING_YBInputSize_STREAMID));

    }
}
