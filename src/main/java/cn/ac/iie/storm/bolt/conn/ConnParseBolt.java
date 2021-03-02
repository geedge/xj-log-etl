package cn.ac.iie.storm.bolt.conn;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.alibaba.fastjson.JSONObject;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.TupleUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import static cn.ac.iie.storm.utils.general.CONNAssignment.*;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class ConnParseBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnParseBolt.class);


    private static final long serialVersionUID = 7483439472002862539L;
    private List<GenericRecord> IPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> HTTPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> FTPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> SSLList = new ArrayList<GenericRecord>();
    private List<GenericRecord> VPNList = new ArrayList<GenericRecord>();
    private List<GenericRecord> DNSList = new ArrayList<GenericRecord>();
    private List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
    private List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();


    private SessionRecordLog sessionRecordLog;
    private long count = 0L;
    private long loadFail = 0L;
    private long connOutputSize = 0L;
    private long connInputSize = 0L;

    private long conn_http=0L;
    private long conn_ip=0L;
    private long conn_ssl=0L;
    private long conn_jstx=0L;
    private long conn_mail=0L;
    private long conn_ftp=0L;
    private long conn_dns=0L;
    private long conn_vpn=0L;

    private Schema vpnSchema;
    private Schema ipSchema;
    private Schema httpSchema;
    private Schema sslSchema;
    private Schema dnsSchema;
    private Schema ftpSchema;
    private Schema mailSchema;
    private Schema jstxSchema;


    public void prepare(Map stormConf, TopologyContext context) {
        ipSchema = new Schema.Parser().parse(StreamAggregateConfig.IP_RZZB_SCHEMA);
        vpnSchema = new Schema.Parser().parse(StreamAggregateConfig.VPN_RZZB_SCHEMA);
        httpSchema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_RZZB_SCHEMA);
        sslSchema = new Schema.Parser().parse(StreamAggregateConfig.SSL_RZZB_SCHEMA);
        ftpSchema = new Schema.Parser().parse(StreamAggregateConfig.FTP_RZZB_SCHEMA);
        dnsSchema = new Schema.Parser().parse(StreamAggregateConfig.DNS_RZZB_SCHEMA);
        mailSchema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_RZZB_SCHEMA);
        jstxSchema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_RZZB_SCHEMA);
    }


    /**
     * 处理一般数据的逻辑业务
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
            if(TupleUtils.isTick(tuple)){

               try {
                    if (!VPNList.isEmpty()){
                        basicOutputCollector.emit(StreamAggregateConfig.CONN_VPN_STREAMID, new Values((Object) generate(vpnSchema, VPNList),VPNList.size()));
                        VPNList.clear();
                    }
                   if (!DNSList.isEmpty()){
                       basicOutputCollector.emit(StreamAggregateConfig.CONN_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList),DNSList.size()));
                       DNSList.clear();
                   }
                   if (!MAILList.isEmpty()){
                       basicOutputCollector.emit(StreamAggregateConfig.CONN_MAIL_STREAMID, new Values((Object) generate(mailSchema, MAILList),MAILList.size()));
                       MAILList.clear();
                   }
                   if (!FTPList.isEmpty()){
                       basicOutputCollector.emit(StreamAggregateConfig.CONN_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList),FTPList.size()));
                       FTPList.clear();
                   }
                   if (!JSTXList.isEmpty()){
                       basicOutputCollector.emit(StreamAggregateConfig.CONN_JSTX_STREAMID, new Values((Object) generate(jstxSchema, JSTXList),JSTXList.size()));
                       JSTXList.clear();
                   }
                }catch (Exception e){
                    e.printStackTrace();
                    logger.error("----------批数据发送异常-----------" + e);
                }
                basicOutputCollector.emit(StreamAggregateConfig.CONN_YBInputSize_STREAMID, new Values(connInputSize,count,loadFail,connOutputSize));
                connInputSize = 0;
                count = 0;
                loadFail = 0;
                connOutputSize = 0;

            }else {
                try {
                    String line = tuple.getString(0);
                    sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
                    connInputSize =connInputSize + sessionRecordLog.getCommon_s2c_byte_num();
                    connOutputSize = connOutputSize + sessionRecordLog.getCommon_c2s_byte_num();
                    count = count + 1;
                    //按common_schema_type的值对数据进行分类发
                switch ( sessionRecordLog.getCommon_schema_type()) {
                    case "BASE":
                        if (sessionRecordLog.getCommon_l7_protocol().equals("PPTP") || sessionRecordLog.getCommon_l7_protocol().equals("L2TP") || sessionRecordLog.getCommon_l7_protocol().equals("OPENVPN") || sessionRecordLog.getCommon_l7_protocol().equals("IPSEC")) {
                            sessionRecordLog.setCommon_schema_type("VPN");
                            try {
                                    GenericRecord vpn_rzzb = new GenericData.Record(vpnSchema);
                                    setVPN_RZZB(vpn_rzzb, sessionRecordLog);
                                    VPNList.add(vpn_rzzb);
                                if (VPNList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                    basicOutputCollector.emit(StreamAggregateConfig.CONN_VPN_STREAMID, new Values((Object) generate(vpnSchema, VPNList),VPNList.size()));
                                    VPNList.clear();
                                }
                            } catch (Exception e) {
                                logger.error("-----------vpn_rzzb数据封装处理异常-----------" + e);
                                loadFail++;
                            }
                        }else if (sessionRecordLog.getCommon_l7_protocol().equals("FTP")){
                                try {
                                    sessionRecordLog.setCommon_schema_type("FTP");
                                    GenericRecord ftp_rzzb = new GenericData.Record(ftpSchema);
                                    setFTP_RZZB(ftp_rzzb, sessionRecordLog);
                                    FTPList.add(ftp_rzzb);
                                    if (FTPList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList),FTPList.size()));
                                        FTPList.clear();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("-----------ftp_rzzb数据封装处理异常-----------" + e);
                                    loadFail++;
                                }

                        } else {
                            sessionRecordLog.setCommon_schema_type("IP");
                            try {
                                    GenericRecord ip_rzzb = new GenericData.Record(ipSchema);
                                    setIP_RZZB(ip_rzzb, sessionRecordLog);
                                    IPList.add(ip_rzzb);
                                if (IPList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                    basicOutputCollector.emit(StreamAggregateConfig.CONN_IP_STREAMID, new Values((Object) generate(ipSchema, IPList),IPList.size()));
                                    IPList.clear();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.error("-----------ip_rzzb数据封装处理异常-----：------"+e );
                                loadFail++;
                            }
                        }
                        break;
                    case "HTTP":
                        sessionRecordLog.setCommon_schema_type("HTTP");
                        try {
                                GenericRecord http_rzzb = new GenericData.Record(httpSchema);
                                setHTTP_RZZB(http_rzzb, sessionRecordLog);
                                HTTPList.add(http_rzzb);

                            if (HTTPList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.CONN_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList),HTTPList.size()));
                                HTTPList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------http_rzzb数据封装处理异常-----------"+e);
                            loadFail++;
                        }
                        break;
                    case "SSL":
                        sessionRecordLog.setCommon_schema_type("SSL");
                        try {
                            GenericRecord ssl_rzzb = new GenericData.Record(sslSchema);

                                setSSL_RZZB(ssl_rzzb, sessionRecordLog);
                                SSLList.add(ssl_rzzb);
                            if (SSLList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.CONN_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList),SSLList.size()));
                                SSLList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ssl_rzzb数据封装处理异常-----------" + e);
                            loadFail++;
                        }
                        break;
                    case "DNS":
                        sessionRecordLog.setCommon_schema_type("DNS");
                        try {
                                GenericRecord dns_rzzb = new GenericData.Record(dnsSchema);
                                setDNS_RZZB(dns_rzzb, sessionRecordLog);
                                DNSList.add(dns_rzzb);
                            if (DNSList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.CONN_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList),DNSList.size()));
                                DNSList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------dns_rzzb数据封装处理异常-----------" + e);
                            loadFail++;
                        }
                        break;
                    case "MAIL":
                        sessionRecordLog.setCommon_schema_type("MAIL");
                        try {
                                GenericRecord mail_rzzb = new GenericData.Record(mailSchema);
                                setMAIL_RZZB(mail_rzzb, sessionRecordLog);
                                MAILList.add(mail_rzzb);
                            if (MAILList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.CONN_MAIL_STREAMID, new Values((Object) generate(mailSchema, MAILList),MAILList.size()));
                                MAILList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_rzzb数据封装处理异常-----------" + e +"异常数据为:"+line);
                            loadFail++;
                        }
                        break;
                    case "APP":
                        sessionRecordLog.setCommon_schema_type("JSTX");
                        try {
                                GenericRecord jstx_rzzb = new GenericData.Record(jstxSchema);
                                setJSTX_RZZB(jstx_rzzb, sessionRecordLog);
                                JSTXList.add(jstx_rzzb);
                            if (JSTXList.size() >= StreamAggregateConfig.CONN_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.CONN_JSTX_STREAMID, new Values((Object) generate(jstxSchema, JSTXList),JSTXList.size()));
                                JSTXList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------jstx_rzzb数据封装处理异常-----------" + e);
                            loadFail++;
                        }
                        break;
                    default:
                        break;
                }
                } catch(Exception e){
                    logger.error("通联数据格式错误"+e);
                }
            }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_VPN_STREAMID, new Fields(StreamAggregateConfig.CONN_VPN_STREAMID,"vpncount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_IP_STREAMID, new Fields(StreamAggregateConfig.CONN_IP_STREAMID,"ipcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_DNS_STREAMID, new Fields(StreamAggregateConfig.CONN_DNS_STREAMID,"dnscount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_HTTP_STREAMID, new Fields(StreamAggregateConfig.CONN_HTTP_STREAMID,"httpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_MAIL_STREAMID, new Fields(StreamAggregateConfig.CONN_MAIL_STREAMID,"mailcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_JSTX_STREAMID, new Fields(StreamAggregateConfig.CONN_JSTX_STREAMID,"jstxcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_FTP_STREAMID, new Fields(StreamAggregateConfig.CONN_FTP_STREAMID,"ftpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_SSL_STREAMID, new Fields(StreamAggregateConfig.CONN_SSL_STREAMID,"sslcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_YBInputSize_STREAMID, new Fields("connInputSize","count","loadFail","connOutputSize"));
    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.CONN_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
