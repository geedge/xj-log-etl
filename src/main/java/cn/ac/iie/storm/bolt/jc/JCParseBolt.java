package cn.ac.iie.storm.bolt.jc;

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

import static cn.ac.iie.storm.utils.general.JCAssignment.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getYL5;

public class JCParseBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCParseBolt.class);
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
    private List<GenericRecord> picList = new ArrayList<GenericRecord>();
    private List<GenericRecord> mediaList = new ArrayList<GenericRecord>();
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
    private Schema picSchema;
    private Schema mediaSchema;
    private String line;



    String hosBucketName = null;
    String fileName = null;



    public void prepare(Map stormConf, TopologyContext context) {
        dnsSchema = new Schema.Parser().parse(StreamAggregateConfig.DNS_JCZB_SCHEMA);
        ftpSchema = new Schema.Parser().parse(StreamAggregateConfig.FTP_JCZB_SCHEMA);
        httpSchema = new Schema.Parser().parse(StreamAggregateConfig.HTTP_JCZB_SCHEMA);
        ipSchema = new Schema.Parser().parse(StreamAggregateConfig.IP_JCZB_SCHEMA);
        jstxSchema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_JCZB_SCHEMA);
        mailSchema = new Schema.Parser().parse(StreamAggregateConfig.MAIL_JCZB_SCHEMA);
        mediaSchema = new Schema.Parser().parse(StreamAggregateConfig.MEDIA_JCZB_SCHEMA);
        picSchema = new Schema.Parser().parse(StreamAggregateConfig.PIC_JCZB_SCHEMA);
        sslSchema = new Schema.Parser().parse(StreamAggregateConfig.SSL_JCZB_SCHEMA);
        vpnSchema = new Schema.Parser().parse(StreamAggregateConfig.VPN_JCZB_SCHEMA);
        wygjcSchema = new Schema.Parser().parse(StreamAggregateConfig.WYGJZ_JCZB_SCHEMA);

    }

    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //首先获取一条数据
        if (TupleUtils.isTick(tuple)) {
            try {
                if (!ipList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_IP_STREAMID, new Values(generate(ipSchema, ipList),ipList.size()));
                    ipList.clear();
                }
                if (!httpList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_HTTP_STREAMID, new Values(generate(httpSchema, httpList),httpList.size()));
                    httpList.clear();
                }

                if (!sslList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_SSL_STREAMID, new Values(generate(sslSchema, sslList),sslList.size()));
                    sslList.clear();
                }
                if (!dnsList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_DNS_STREAMID, new Values(generate(dnsSchema, dnsList),dnsList.size()));
                    dnsList.clear();
                }
                if (!(wygjcList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_WYGJC_STREAMID, new Values(generate(wygjcSchema,wygjcList),wygjcList.size()));
                    wygjcList.clear();
                }
                if (!(jstxList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_JSTX_STREAMID, new Values(generate(jstxSchema,jstxList),jstxList.size()));
                    jstxList.clear();
                }
                if (!(ftpList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_FTP_STREAMID, new Values(generate(ftpSchema,ftpList),ftpList.size()));
                    ftpList.clear();
                }
                if (!(picList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_PIC_STREAMID, new Values(generate(picSchema,picList),picList.size()));
                    picList.clear();
                }
                if (!(mediaList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_MEDIA_STREAMID, new Values(generate(mediaSchema,mediaList),mediaList.size()));
                    mediaList.clear();
                }
                 if(!(mailList.isEmpty())) {
                     basicOutputCollector.emit(StreamAggregateConfig.JC_MAIL_STREAMID, new Values(generate(mailSchema,mailList),mailList.size()));
                     mailList.clear();
                 }
                if (!vpnList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_VPN_STREAMID, new Values(generate(vpnSchema, vpnList),vpnList.size()));
                    vpnList.clear();
                }
                basicOutputCollector.emit(StreamAggregateConfig.JC_COUNT_STREAMID, new Values(count,loadfail));
             }catch (Exception e) {
                logger.error("----------批数据发送异常-----------" +e);
            }
            count = 0L;
            loadfail = 0L;

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
//弃用
//                sessionRecordLog.setYyzl(traffic_forward.getYyzl());
//                sessionRecordLog.setGjc(traffic_forward.getGjc());
//                sessionRecordLog.setZsmc(traffic_forward.getZsmc());
//                sessionRecordLog.setYjxy(traffic_forward.getYjxy());
//                sessionRecordLog.setProtocol(userRegion.getProtocol());

                int pzlx = traffic_forward.getPzlx();

                //按pzlx的值对数据进行分类发送
                switch (pzlx) {
                    case 501:   //IP监测
                        try {
                            count ++;
                            GenericRecord ip_jcrz = new GenericData.Record(ipSchema);
                            sessionRecordLog.setCommon_schema_type("IP");
                            setIP_JCRZ(ip_jcrz, sessionRecordLog);
                            ipList.add(ip_jcrz);
                        if (ipList.size() >=  StreamAggregateConfig.BATCH_INTER_NUM) {
                            basicOutputCollector.emit(StreamAggregateConfig.JC_IP_STREAMID, new Values(generate(ipSchema, ipList),ipList.size()));
                            ipList.clear();
                         }
                          } catch (Exception e) {
                             e.printStackTrace();
                             logger.error("-----------ip_jcrz数据封装处理异常-----------" + e);
                             loadfail++;
                     }
                         break;
                    case 502:   //DNS监测
                        try {
                            count++;
                            GenericRecord dns_jczb = new GenericData.Record(dnsSchema);
                            sessionRecordLog.setCommon_schema_type("DNS");
                            setDNS_JCZB(dns_jczb, sessionRecordLog);
                            if (dns_jczb.get("DNS_NAME")!=("")){
                                dnsList.add(dns_jczb);
                            }else {
                                loadfail++;
                            }
                            if (dnsList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_DNS_STREAMID, new Values(generate(dnsSchema, dnsList),dnsList.size()));
                                dnsList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------dns_jczb数据封装处理异常-----------" + line);
                            loadfail++;
                        }
                        break;
                    case 503:   //URL监测
                    case 504:   //网站监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("jc-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_HTTP_JCZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_HTTP_JCZB, fileName));
                            }
                            GenericRecord http_jczb = new GenericData.Record(httpSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setHTTP_JCZB(http_jczb, sessionRecordLog);
                            httpList.add(http_jczb);
                            if (httpList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_HTTP_STREAMID, new Values(generate(httpSchema, httpList),httpList.size()));
                                httpList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------http_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 505:   //特定证书监测
                        try {
                            count++;
                            sessionRecordLog.setZsmc(traffic_forward.getZsmc());
                            GenericRecord ssl_jczb = new GenericData.Record(sslSchema);
                            sessionRecordLog.setCommon_schema_type("SSL");
                            setSSL_JCZB(ssl_jczb, sessionRecordLog);
                            sslList.add(ssl_jczb);
                            if (sslList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_SSL_STREAMID, new Values(generate(sslSchema, sslList),sslList.size()));
                                sslList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ssl_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 506:   //网站关键词监测
                    case 509:   //搜索词监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("jc-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_WYGJC_JCRZ));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_WYGJC_JCRZ, fileName));
                            }
                            GenericRecord wygjc_jczb = new GenericData.Record(wygjcSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setWYGJC_JCZB(wygjc_jczb, sessionRecordLog);
                            wygjcList.add(wygjc_jczb);
                            if (wygjcList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_WYGJC_STREAMID, new Values(generate(wygjcSchema,wygjcList),wygjcList.size()));
                                wygjcList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------wygjc_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 507:   //邮件监测
                    case 510:   //邮件监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            sessionRecordLog.setYjxy(traffic_forward.getYjxy());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("jc-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_MAIL_JCZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_MAIL_JCZB, fileName));
                            }
                            GenericRecord mail_jczb = new GenericData.Record(mailSchema);
                            sessionRecordLog.setCommon_schema_type("MAIL");
                            setMAIL_JCZB(mail_jczb, sessionRecordLog);
                            mailList.add(mail_jczb);
                            if (mailList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_MAIL_STREAMID, new Values(generate(mailSchema,mailList),mailList.size()));
                                mailList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 508:   //FTP关键词监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setGjc(traffic_forward.getGjc());
                            GenericRecord ftp_jczb = new GenericData.Record(ftpSchema);
                            sessionRecordLog.setCommon_schema_type("FTP");
                            setFTP_JCZB(ftp_jczb,sessionRecordLog);
                            ftpList.add(ftp_jczb);
                            if (mailList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_FTP_STREAMID, new Values(generate(ftpSchema,mailList),ftpList.size()));
                                ftpList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ftp_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 511:   //VPN监测
                        try {
                            count++;
                            GenericRecord vpn_jczb = new GenericData.Record(vpnSchema);
                            sessionRecordLog.setCommon_schema_type("VPN");
                            sessionRecordLog.setLx(traffic_forward.getLx());
                            setVPN_JCZB(vpn_jczb, sessionRecordLog);
                            if(StringUtil.isNotEmpty(vpn_jczb.get("DETAIL_ID"))) {
                                vpnList.add(vpn_jczb);
                            }else {
                                logger.error("vpnjc流量DETAIL_ID为空具体日志为:"+line);
                                loadfail++;
                            }
                            if (vpnList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_VPN_STREAMID, new Values(generate(vpnSchema,vpnList),vpnList.size()));
                                vpnList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------vpn_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 512:   //特定应用监测
                    case 513:   //社交应用监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            sessionRecordLog.setProtocol(userRegion.getProtocol());
                            GenericRecord jstx_jczb = new GenericData.Record(jstxSchema);
                            sessionRecordLog.setProtocol(userRegion.getProtocol());
                            sessionRecordLog.setCommon_schema_type("JSTX");
                            setJSTX_JCZB(jstx_jczb, sessionRecordLog);
                            jstxList.add(jstx_jczb);
                            if (jstxList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_JSTX_STREAMID, new Values(generate(jstxSchema,jstxList),jstxList.size()));
                                jstxList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------jstx_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 514:   //图片监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            GenericRecord pic_jczb = new GenericData.Record(picSchema);
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("jc-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_PIC_JCZB));
                                sessionRecordLog.setHttp_response_body(getYL5( StreamAggregateConfig.T_PIC_JCZB,fileName));
                            }
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setPIC_JCZB(pic_jczb, sessionRecordLog);
                            picList.add(pic_jczb);
                            if (picList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_PIC_STREAMID, new Values(generate(picSchema,picList),picList.size()));
                                picList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------pic_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 515:   //多媒体监测
                        try {
                            count++;
                            sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                            if (sessionRecordLog.getHttp_response_body()!=null){
                                String[] split = sessionRecordLog.getHttp_response_body().split("/");
                                hosBucketName = split[split.length-2];
                                fileName = split[split.length-1];
                                basicOutputCollector.emit("jc-unstructured",new Values(hosBucketName,fileName,StreamAggregateConfig.T_MEDIA_JCZB));
                                sessionRecordLog.setHttp_response_body(getYL5(StreamAggregateConfig.T_MEDIA_JCZB, fileName));
                            }
                            GenericRecord media_jczb = new GenericData.Record(mediaSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setMEDIA_JCZB(media_jczb, sessionRecordLog);
                            mediaList.add(media_jczb);
                            if (mediaList.size() >= StreamAggregateConfig.BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_MEDIA_STREAMID, new Values(generate(mediaSchema,mediaList),mediaList.size()));
                                mediaList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------media_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    default:
                        break;
                }
            } catch(Exception e){
                  logger.error("JC数据处理异常"+e+"异常数据为:"+line);
                  loadfail++;
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_IP_STREAMID,new Fields(StreamAggregateConfig.JC_IP_STREAMID,"ipcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_DNS_STREAMID,new Fields(StreamAggregateConfig.JC_DNS_STREAMID,"dnscount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_HTTP_STREAMID,new Fields(StreamAggregateConfig.JC_HTTP_STREAMID,"httpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_SSL_STREAMID,new Fields(StreamAggregateConfig.JC_SSL_STREAMID,"sslcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_WYGJC_STREAMID,new Fields(StreamAggregateConfig.JC_WYGJC_STREAMID,"wygjccount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MAIL_STREAMID,new Fields(StreamAggregateConfig.JC_MAIL_STREAMID,"mailcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_FTP_STREAMID,new Fields(StreamAggregateConfig.JC_FTP_STREAMID,"ftpcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_VPN_STREAMID,new Fields(StreamAggregateConfig.JC_VPN_STREAMID,"vpncount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_JSTX_STREAMID,new Fields(StreamAggregateConfig.JC_JSTX_STREAMID,"jstxcount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_PIC_STREAMID,new Fields(StreamAggregateConfig.JC_PIC_STREAMID,"piccount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MEDIA_STREAMID,new Fields(StreamAggregateConfig.JC_MEDIA_STREAMID,"mediacount"));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_COUNT_STREAMID, new Fields("count","loadfail"));
        outputFieldsDeclarer.declareStream("jc-unstructured", new Fields("hosBucketName","fileName","s3BucketName"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
