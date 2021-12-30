package cn.ac.iie.storm.bolt.conn;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.alibaba.fastjson.JSONObject;

import com.zdjizhi.utils.StringUtil;
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


import static cn.ac.iie.storm.utils.general.GetMap.getDy;
import static cn.ac.iie.storm.utils.general.CONNAssignment.*;

import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

import static cn.ac.iie.storm.utils.http.HttpGetUrl.getYL5;

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

    private long  yys_error=0L;

    private long ydy_inside=0L;
    private long mddy_inside=0L;

    private long ydy_outside=0L;
    private long mddy_outside=0L;

    private long ydy_unknown=0L;
    private long mddy_unknown=0L;


    private Schema vpnSchema;
    private Schema ipSchema;
    private Schema httpSchema;
    private Schema sslSchema;
    private Schema dnsSchema;
    private Schema ftpSchema;
    private Schema mailSchema;
    private Schema jstxSchema;

    String bucketName = null;
    String filename = null;

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

                basicOutputCollector.emit(StreamAggregateConfig.CONN_YBInputSize_STREAMID, new Values(connInputSize,count,loadFail,connOutputSize,yys_error,ydy_inside,ydy_outside,ydy_unknown,mddy_inside,mddy_outside,mddy_unknown));
                connInputSize = 0;
                count = 0;
                loadFail = 0;
                connOutputSize = 0;
                ydy_inside=0L;
                mddy_inside=0L;
                ydy_outside=0L;
                mddy_outside=0L;
                ydy_unknown=0L;
                mddy_unknown=0L;
                yys_error=0L;

            }else {
                try {
                    String line = tuple.getString(0);
                    sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
                    if(StringUtil.isBlank(sessionRecordLog.getCommon_data_center())){
                        yys_error++;
                    }
                    sessionRecordLog.setYys(getYys(sessionRecordLog.getCommon_data_center()));
                    sessionRecordLog.setYdy(getDy(sessionRecordLog.getCommon_client_location()));
                    sessionRecordLog.setMddy(getDy(sessionRecordLog.getCommon_server_location()));
                    if(sessionRecordLog.getYys()!=StreamAggregateConfig.YYS_NUM){
                        yys_error++;
                    }

                    if (StringUtil.startsWith(sessionRecordLog.getYdy(),"65")&&sessionRecordLog.getYdy().length()==6){
                        ydy_inside++;
                    }else if (sessionRecordLog.getYdy().equals("0")){
                        ydy_unknown++;
                    }else {
                        ydy_outside++;
                    }

                    if (StringUtil.startsWith(sessionRecordLog.getMddy(),"65")&&sessionRecordLog.getMddy().length()==6){
                        mddy_inside++;
                    }else if(sessionRecordLog.getMddy().equals("0")){
                        mddy_unknown++;
                    }else {
                        mddy_outside++;
                    }

                    connInputSize = connInputSize + sessionRecordLog.getCommon_s2c_byte_num();
                    connOutputSize = connOutputSize + sessionRecordLog.getCommon_c2s_byte_num();

                    //按common_schema_type的值对数据进行分类发
                    if (sessionRecordLog.getCommon_app_label()!=null&&StringUtil.startsWith(sessionRecordLog.getCommon_app_label(),"05")) {
                            sessionRecordLog.setCommon_schema_type("JSTX");
                            try {
                                count++;
                                GenericRecord jstx_rzzb = new GenericData.Record(jstxSchema);
                                setJSTX_RZZB(jstx_rzzb, sessionRecordLog);
                                JSTXList.add(jstx_rzzb);
                                if (JSTXList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                    basicOutputCollector.emit(StreamAggregateConfig.CONN_JSTX_STREAMID, new Values((Object) generate(jstxSchema, JSTXList), JSTXList.size()));
                                    JSTXList.clear();
                                }
                            } catch (Exception e) {
                                e.printStackTrace();
                                logger.error("-----------jstx_rzzb数据封装处理异常-----------" + e);
                                loadFail++;
                            }
                    } else {
                        switch (sessionRecordLog.getCommon_schema_type()) {
                            case "BASE":
                                if (sessionRecordLog.getCommon_l7_protocol().equals("PPTP") || sessionRecordLog.getCommon_l7_protocol().equals("L2TP") || sessionRecordLog.getCommon_l7_protocol().equals("OPENVPN") || sessionRecordLog.getCommon_l7_protocol().equals("ISAKMP")
                                  || sessionRecordLog.getCommon_protocol_label().contains("PPTP")||sessionRecordLog.getCommon_protocol_label().contains("L2TP")||sessionRecordLog.getCommon_protocol_label().contains("OPENVPN")||sessionRecordLog.getCommon_protocol_label().contains("IPSEC")){
                                    sessionRecordLog.setCommon_schema_type("VPN");
                                    try {
                                        count++;
                                        GenericRecord vpn_rzzb = new GenericData.Record(vpnSchema);
                                        setVPN_RZZB(vpn_rzzb, sessionRecordLog);
                                        VPNList.add(vpn_rzzb);
                                        if (VPNList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                            basicOutputCollector.emit(StreamAggregateConfig.CONN_VPN_STREAMID, new Values((Object) generate(vpnSchema, VPNList), VPNList.size()));
                                            VPNList.clear();
                                        }
                                    } catch (Exception e) {
                                        logger.error("-----------vpn_rzzb数据封装处理异常-----------" + e);
                                        loadFail++;
                                    }
                                } else {
                                    sessionRecordLog.setCommon_schema_type("IP");
                                    try {
                                        count++;
                                        GenericRecord ip_rzzb = new GenericData.Record(ipSchema);
                                        setIP_RZZB(ip_rzzb, sessionRecordLog);
                                        IPList.add(ip_rzzb);
                                        if (IPList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                            basicOutputCollector.emit(StreamAggregateConfig.CONN_IP_STREAMID, new Values((Object) generate(ipSchema, IPList), IPList.size()));
                                            IPList.clear();
                                        }
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                        logger.error("-----------ip_rzzb数据封装处理异常-----：------" + e);
                                        loadFail++;
                                    }
                                }
                                break;
                            case "FTP":
                                try {
                                    count++;
                                    GenericRecord ftp_rzzb = new GenericData.Record(ftpSchema);
                                    setFTP_RZZB(ftp_rzzb, sessionRecordLog);
                                    FTPList.add(ftp_rzzb);
                                    if (FTPList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList), FTPList.size()));
                                        FTPList.clear();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("-----------ftp_rzzb数据封装处理异常-----------" + e);
                                    loadFail++;
                                }
                            case "HTTP":
                                try {
                                    if (sessionRecordLog.getHttp_response_body() != null) {
                                        String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                        bucketName = split[split.length - 2];
                                        filename = split[split.length - 1];
                                        basicOutputCollector.emit("conn-unstructured", new Values(bucketName, filename, StreamAggregateConfig.T_HTTP_RZZB));
                                        sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_HTTP_RZZB, filename));
                                    }
                                    count++;
                                    GenericRecord http_rzzb = new GenericData.Record(httpSchema);
                                    sessionRecordLog.setCommon_schema_type("HTTP");
                                    setHTTP_RZZB(http_rzzb, sessionRecordLog);
                                    HTTPList.add(http_rzzb);
                                    if (HTTPList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList), HTTPList.size()));
                                        HTTPList.clear();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("-----------http_rzzb数据封装处理异常-----------" + e);
                                    loadFail++;
                                }
                                break;
                            case "SSL":
                                sessionRecordLog.setCommon_schema_type("SSL");
                                try {
                                    count++;
                                    GenericRecord ssl_rzzb = new GenericData.Record(sslSchema);
                                    setSSL_RZZB(ssl_rzzb, sessionRecordLog);
                                    SSLList.add(ssl_rzzb);
                                    if (SSLList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList), SSLList.size()));
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
                                    count++;
                                    GenericRecord dns_rzzb = new GenericData.Record(dnsSchema);
                                    setDNS_RZZB(dns_rzzb, sessionRecordLog);
                                    if (dns_rzzb.get("DNS_NAME")!=("")){
                                        DNSList.add(dns_rzzb);
                                    }else {
                                        loadFail++;
                                    }
                                    if (DNSList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList), DNSList.size()));
                                        DNSList.clear();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("-----------dns_rzzb数据封装处理异常-----------" + e);
                                    loadFail++;
                                }
                                break;
                            case "MAIL":
                                try {
                                    if (sessionRecordLog.getHttp_response_body() != null) {
                                        String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                        bucketName = split[split.length - 2];
                                        filename = split[split.length - 1];
                                        basicOutputCollector.emit("conn-unstructured", new Values(bucketName, filename, StreamAggregateConfig.T_MAIL_RZZB));
                                        sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_MAIL_RZZB, filename));
                                    }
                                    count++;
                                    GenericRecord mail_rzzb = new GenericData.Record(mailSchema);
                                    sessionRecordLog.setCommon_schema_type("MAIL");
                                    setMAIL_RZZB(mail_rzzb, sessionRecordLog);
                                    MAILList.add(mail_rzzb);
                                    if (MAILList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                        basicOutputCollector.emit(StreamAggregateConfig.CONN_MAIL_STREAMID, new Values((Object) generate(mailSchema, MAILList), MAILList.size()));
                                        MAILList.clear();
                                    }
                                } catch (Exception e) {
                                    e.printStackTrace();
                                    logger.error("-----------mail_rzzb数据封装处理异常-----------" + e + "异常数据为:" + line);
                                    loadFail++;
                                }
                                break;
                            default:
                                break;
                        }
                    }

                } catch(Exception e){
                    logger.error("通联数据格式错误"+e);
                    loadFail++;
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
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.CONN_YBInputSize_STREAMID, new Fields("connInputSize","count","loadFail","connOutputSize","yys_error","ydy_inside","ydy_outside","ydy_unknown","mddy_inside","mddy_outside","mddy_unknown"));
        outputFieldsDeclarer.declareStream("conn-unstructured", new Fields("bucketName","filename","bucketname"));

    }


    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
