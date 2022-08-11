package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.bean.Traffic_Forward;
import cn.ac.iie.storm.bean.UserRegion;

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

import static cn.ac.iie.storm.utils.general.GKAssignment.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getS3Url;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getYL5;

public class GkParseBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(GkParseBolt.class);
    private static final long serialVersionUID = 7483439472002862539L;

    private SessionRecordLog sessionRecordLog;

    private List<GenericRecord> ipList = new ArrayList<GenericRecord>();
    private List<GenericRecord> httpList = new ArrayList<GenericRecord>();
    private List<GenericRecord> ftpList = new ArrayList<GenericRecord>();
    private List<GenericRecord> sslList = new ArrayList<GenericRecord>();
    private List<GenericRecord> vpnList = new ArrayList<GenericRecord>();
    private List<GenericRecord> dnsList = new ArrayList<GenericRecord>();
    private List<GenericRecord> mailList = new ArrayList<GenericRecord>();
    private List<GenericRecord> jstxList = new ArrayList<GenericRecord>();
    private List<GenericRecord> wygjcList = new ArrayList<GenericRecord>();


    private long count = 0L;
    private long loadfail = 0L;


    private Schema vpnSchema;
    private Schema ipSchema;
    private Schema httpSchema;
    private Schema sslSchema;
    private Schema dnsSchema;
    private Schema ftpSchema;
    private Schema mailSchema;
    private Schema jstxSchema;
    private Schema wygjcSchema;
    private String line;

    String hosBucketName = null;
    String fileName = null;



    public void prepare(Map stormConf, TopologyContext context) {
        dnsSchema = new Schema.Parser().parse(StreamAggregateConfig.DNS_GKZB_SCHEMA);
        ftpSchema = new Schema.Parser().parse(StreamAggregateConfig.FTP_GKZB_SCHEMA);
        httpSchema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_GKZB_SCHEMA);
        ipSchema = new Schema.Parser().parse(StreamAggregateConfig.IP_GKRZ_SCHEMA);
        jstxSchema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_GKZB_SCHEMA);
        mailSchema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_GKZB_SCHEMA);
        sslSchema = new Schema.Parser().parse(StreamAggregateConfig.SSL_GKZB_SCHEMA);
        vpnSchema = new Schema.Parser().parse(StreamAggregateConfig.VPN_GKZB_SCHEMA);
        wygjcSchema = new Schema.Parser().parse(StreamAggregateConfig.WYGJC_GKRZ_SCHEMA);
    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //首先获取一条数据
            if (TupleUtils.isTick(tuple)) {
                try {
                    if (!ipList.isEmpty()){
                        basicOutputCollector.emit(StreamAggregateConfig.GK_IP_STREAMID, new Values(generate(ipSchema, ipList),ipList.size()));
                        ipList.clear();
                    }
                    if (!httpList.isEmpty()){
                        basicOutputCollector.emit(StreamAggregateConfig.GK_HTTP_STREAMID, new Values(generate(httpSchema, httpList),httpList.size()));
                        httpList.clear();
                    }
                    if (!sslList.isEmpty()){
                        basicOutputCollector.emit(StreamAggregateConfig.GK_SSL_STREAMID, new Values((generate(sslSchema, sslList)),sslList.size()));
                        sslList.clear();
                    }
                    if (!dnsList.isEmpty()){
                       basicOutputCollector.emit(StreamAggregateConfig.GK_DNS_STREAMID, new Values(generate(dnsSchema, dnsList),dnsList.size()));
                        dnsList.clear();
                    }

                    if (!(ftpList.isEmpty())) {
                        basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID, new Values(generate(ftpSchema, ftpList),ftpList.size()));
                        ftpList.clear();
                    }
                    if (!(wygjcList.isEmpty())) {
                        basicOutputCollector.emit(StreamAggregateConfig.GK_WYGJC_STREAMID, new Values(generate(wygjcSchema, wygjcList),wygjcList.size()));
                        wygjcList.clear();
                    }
                    if (!(wygjcList.isEmpty())) {
                        basicOutputCollector.emit(StreamAggregateConfig.GK_JSTX_STREAMID, new Values(generate(jstxSchema, wygjcList),wygjcList.size()));
                        wygjcList.clear();
                    }
                    if (!vpnList.isEmpty()) {
                        basicOutputCollector.emit(StreamAggregateConfig.GK_VPN_STREAMID, new Values(generate(vpnSchema, vpnList),vpnList.size()));
                        vpnList.clear();
                    }
                    if (!(mailList.isEmpty())) {
                        basicOutputCollector.emit(StreamAggregateConfig.GK_MAIL_STREAMID, new Values(generate(mailSchema, mailList),mailList.size()));
                        mailList.clear();
                    }
                    basicOutputCollector.emit(StreamAggregateConfig.GK_COUNT_STREAMID, new Values(count,loadfail));

                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("----------批数据发送异常-----------" + e);
                }
                count = 0;
                loadfail = 0;
            } else {
                try {
                line = tuple.getString(0);
                sessionRecordLog = JSONObject.parseObject(line, SessionRecordLog.class);
                UserRegion userRegion = JSONObject.parseObject(sessionRecordLog.getCommon_user_region(), UserRegion.class);
                Traffic_Forward traffic_forward = JSONObject.parseObject(userRegion.getTraffic_forward(), Traffic_Forward.class);
                
                sessionRecordLog.setPzid(traffic_forward.getPzid());
                sessionRecordLog.setFlid(traffic_forward.getFlid());
                sessionRecordLog.setXzid(traffic_forward.getXzid());
                sessionRecordLog.setLhid(traffic_forward.getLhid());
                sessionRecordLog.setPzlx(traffic_forward.getPzlx());
                sessionRecordLog.setGklb(traffic_forward.getGklb());
                sessionRecordLog.setLabels(traffic_forward.getLabels());

//              sessionRecordLog.setZsmc(traffic_forward.getZsmc());
 //             sessionRecordLog.setYjxy(traffic_forward.getYjxy());

                int pzlx = traffic_forward.getPzlx();
                //按pzlx的值对数据进行分类发送
                //按pzlx的值对数据进行分类发送
                 switch (pzlx) {
                    //管控
                    case 201:  //IP黑名单
                    case 601:  //网安IP管控
                    case 701:  //反诈IP管控
                    case 801:  //工业互联网IP管控
                        count++;
                        try {
                            GenericRecord ip_gkrz = new GenericData.Record(ipSchema);
                            sessionRecordLog.setCommon_schema_type("IP");
                            setIP_GKRZ(ip_gkrz, sessionRecordLog);
                            ipList.add(ip_gkrz);
                            if (ipList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_IP_STREAMID, new Values((Object) generate(ipSchema, ipList),ipList.size()));
                                ipList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ip_rzzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 402:   //DNS欺骗
                    case 602:   //网安DNS管控
                    case 702:   //反诈DNS管控
                    case 802:   //工业互联网DNS管控
                        count++;
                        try {
                            GenericRecord dns_gkzb = new GenericData.Record(dnsSchema);
                            sessionRecordLog.setCommon_schema_type("DNS");
                            setDNS_GKZB(dns_gkzb, sessionRecordLog);
                            if (dns_gkzb.get("DNS_NAME")!=("")){
                                dnsList.add(dns_gkzb);
                            }else {
                                loadfail++;
                            }
                            if (dnsList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_DNS_STREAMID, new Values(generate(dnsSchema, dnsList),dnsList.size()));
                                dnsList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------dns_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 403:   //URL封堵
                    case 404:   //网站封堵
                    case 603:   //网安URL管控
                    case 703:   //反诈URL管控
                        count++;
                        try {
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("gk-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_MAIL_GKZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_HTTP_GKZB, fileName));
                            }
                            GenericRecord http_gkzb = new GenericData.Record(httpSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setHTTP_GKZB(http_gkzb, sessionRecordLog);
                            httpList.add(http_gkzb);
                            if (httpList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_HTTP_STREAMID, new Values(generate(httpSchema, httpList),httpList.size()));
                                httpList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------http_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 405:   //特定证书封堵
                        count++;
                        try {
                            sessionRecordLog.setZsmc(traffic_forward.getZsmc());
                            GenericRecord ssl_gkzb = new GenericData.Record(sslSchema);
                            sessionRecordLog.setCommon_schema_type("SSL");
                            setSSL_GKZB(ssl_gkzb, sessionRecordLog);
                            sslList.add(ssl_gkzb);
                            if (sslList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_SSL_STREAMID, new Values(generate(sslSchema, sslList),sslList.size()));
                                sslList.clear();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ssl_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }

                        break;
                    case 406:   //网站关键字封堵
                    case 409:   //搜索词封堵
                        count++;
                        try {
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("gk-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_MAIL_GKZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_WYGJC_GKRZ, fileName));
                            }
                            GenericRecord wygjc_gkzb = new GenericData.Record(wygjcSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setWYGJC_GKZB(wygjc_gkzb, sessionRecordLog);
                            wygjcList.add(wygjc_gkzb);
                            if (wygjcList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_WYGJC_STREAMID, new Values(generate(wygjcSchema, wygjcList),wygjcList.size()));
                                wygjcList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------wygjc_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 407:   //邮件关键字封堵
                    case 410:
                        count++;
                        try {
                            sessionRecordLog.setYjxy(traffic_forward.getYjxy());
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("gk-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_MAIL_GKZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_MAIL_GKZB, fileName));
                            }
                            GenericRecord mail_gkzb = new GenericData.Record(mailSchema);
                            sessionRecordLog.setCommon_schema_type("MAIL");
                            setMAIL_GKZB(mail_gkzb, sessionRecordLog);
                            mailList.add(mail_gkzb);
                            if (mailList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_MAIL_STREAMID, new Values(generate(mailSchema, mailList),mailList.size()));
                                mailList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_gkzb数据封装处理异常-----------" + e + "异常数据为"+line);
                            loadfail++;
                        }
                        break;
                    case 408:   //FTP关键字封堵
                        count++;
                        try {
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            GenericRecord ftp_gkzb = new GenericData.Record(ftpSchema);
                            sessionRecordLog.setCommon_schema_type("FTP");
                            setFTP_GKZB(ftp_gkzb, sessionRecordLog);
                            ftpList.add(ftp_gkzb);
                            if (ftpList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID, new Values(generate(ftpSchema, ftpList),ftpList.size()));
                                ftpList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 411:   //VPN封堵
                        count++;
                        try {
                            GenericRecord vpn_gkzb = new GenericData.Record(vpnSchema);
                            sessionRecordLog.setLx(traffic_forward.getLx());
                            sessionRecordLog.setCommon_schema_type("VPN");
                            setVPN_GKZB(vpn_gkzb, sessionRecordLog);
                            if(StringUtil.isNotEmpty(vpn_gkzb.get("DETAIL_ID"))) {
                                vpnList.add(vpn_gkzb);
                            }else {
                                logger.error("vpnjc流量DETAIL_ID为空具体日志为:"+line);
                                loadfail++;
                            }
                            if (vpnList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_VPN_STREAMID, new Values(generate(vpnSchema, vpnList),vpnList.size()));
                                vpnList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------vpn_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 412:   //特定应用封堵
                    case 413:   //社交应用封堵
                        count++;
                        try {
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setProtocol(userRegion.getProtocol());
                            GenericRecord jstx_gkzb = new GenericData.Record(jstxSchema);
                            sessionRecordLog.setCommon_schema_type("JSTX");
                            setJSTX_GKZB(jstx_gkzb, sessionRecordLog);
                            jstxList.add(jstx_gkzb);
                            if (jstxList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_JSTX_STREAMID, new Values(generate(jstxSchema, jstxList),jstxList.size()));
                                jstxList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("----------jstx_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    default:
                        break;
                }
                } catch(Exception e){
                      logger.error("GK数据处理异常"+e+"异常数据"+line);
                }
            }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_IP_STREAMID,new Fields(StreamAggregateConfig.GK_IP_STREAMID,"ipcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_DNS_STREAMID,new Fields(StreamAggregateConfig.GK_DNS_STREAMID,"dnscount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_HTTP_STREAMID,new Fields(StreamAggregateConfig.GK_HTTP_STREAMID,"httpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_SSL_STREAMID,new Fields(StreamAggregateConfig.GK_SSL_STREAMID,"sslcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_WYGJC_STREAMID,new Fields(StreamAggregateConfig.GK_WYGJC_STREAMID,"wygjccount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_MAIL_STREAMID,new Fields(StreamAggregateConfig.GK_MAIL_STREAMID,"mailcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_FTP_STREAMID,new Fields(StreamAggregateConfig.GK_FTP_STREAMID,"ftpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_VPN_STREAMID,new Fields(StreamAggregateConfig.GK_VPN_STREAMID,"vpncount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_JSTX_STREAMID,new Fields(StreamAggregateConfig.GK_JSTX_STREAMID,"jstxcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_COUNT_STREAMID, new Fields("count","loadfail"));
        outputFieldsDeclarer.declareStream("gk-unstructured", new Fields("hosBucketName","fileName","s3BucketName"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
