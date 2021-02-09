package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.bean.Traffic_Forward;
import cn.ac.iie.storm.bean.UserRegion;
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

import static cn.ac.iie.storm.utils.general.JCAssignment.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getValidLength;

public class JCParseBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JCParseBolt.class);
    private static final long serialVersionUID = 7483439472002862539L;

    private SessionRecordLog sessionRecordLog;




    private List<GenericRecord> IPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> HTTPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> FTPList = new ArrayList<GenericRecord>();
    private List<GenericRecord> SSLList = new ArrayList<GenericRecord>();
    private List<GenericRecord> VPNList = new ArrayList<GenericRecord>();
    private List<GenericRecord> DNSList = new ArrayList<GenericRecord>();
    private List<GenericRecord> MAILList = new ArrayList<GenericRecord>();
    private List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();
    private List<GenericRecord> WYGJCList = new ArrayList<GenericRecord>();
    private List<GenericRecord> PICList = new ArrayList<GenericRecord>();
    private List<GenericRecord> MEDIAList = new ArrayList<GenericRecord>();





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
                if (!IPList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_IP_STREAMID, new Values((Object) generate(ipSchema, IPList)));
                    IPList.clear();
                }


                if (!HTTPList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList)));
                    HTTPList.clear();
                }

                if (!SSLList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList)));
                    SSLList.clear();
                }
                if (!DNSList.isEmpty()){
                    basicOutputCollector.emit(StreamAggregateConfig.JC_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList)));
                    DNSList.clear();
                }

                if (!(WYGJCList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_WYGJC_STREAMID, new Values((Object) generate(wygjcSchema,WYGJCList)));
                    WYGJCList.clear();
                }
                if (!(JSTXList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_JSTX_STREAMID, new Values((Object) generate(jstxSchema,VPNList)));
                    JSTXList.clear();
                }
                if (!(FTPList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_FTP_STREAMID, new Values((Object) generate(ftpSchema,MAILList)));
                    FTPList.clear();
                }
                if (!(PICList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_PIC_STREAMID, new Values((Object) generate(picSchema,PICList)));
                    PICList.clear();
                }
                if (!(MEDIAList.isEmpty())) {
                    basicOutputCollector.emit(StreamAggregateConfig.JC_MEDIA_STREAMID, new Values((Object) generate(mediaSchema,MEDIAList)));
                    MEDIAList.clear();
                }
                 if(!(MAILList.isEmpty())) {
                     basicOutputCollector.emit(StreamAggregateConfig.JC_MAIL_STREAMID, new Values((Object) generate(mailSchema,MAILList)));
                     MAILList.clear();
                 }
                basicOutputCollector.emit(StreamAggregateConfig.JC_COUNT_STREAMID, new Values(count,loadfail));
             }catch (Exception e) {
                logger.error("----------批数据发送异常-----------" +e);
            }
            count = 0;
            loadfail = 0;

        } else {
            try {
                String line = tuple.getString(0);
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
                sessionRecordLog.setYyzl(traffic_forward.getYyzl());
                int pzlx = traffic_forward.getPzlx();
                //按pzlx的值对数据进行分类发送
                //按pzlx的值对数据进行分类发送
                switch (pzlx) {
                    case 501:   //IP监测
                        try {
                            GenericRecord ip_jcrz = new GenericData.Record(ipSchema);
                            sessionRecordLog.setCommon_schema_type("IP");
                            setIP_JCRZ(ip_jcrz, sessionRecordLog);
                            IPList.add(ip_jcrz);
                            count++;
                        if (IPList.size() >=  StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                            basicOutputCollector.emit(StreamAggregateConfig.JC_IP_STREAMID, new Values((Object) generate(ipSchema, IPList)));
                            IPList.clear();
                         }
                          } catch (Exception e) {
                             e.printStackTrace();
                             logger.error("-----------ip_jcrz数据封装处理异常-----------" + e);
                             loadfail++;
                     }
                         break;
                    case 502:   //DNS监测
                        try {
                            GenericRecord dns_jczb = new GenericData.Record(dnsSchema);
                            sessionRecordLog.setCommon_schema_type("DNS");
                            setDNS_JCZB(dns_jczb, sessionRecordLog);
                            DNSList.add(dns_jczb);
                            count++;
                            if (DNSList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList)));
                                DNSList.clear();
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
                            GenericRecord http_jczb = new GenericData.Record(httpSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setHTTP_JCZB(http_jczb, sessionRecordLog);
                            HTTPList.add(http_jczb);
                            count++;
                            if (HTTPList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList)));
                                HTTPList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------http_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 505:   //特定证书监测
                        try {
                            GenericRecord ssl_jczb = new GenericData.Record(sslSchema);
                            sessionRecordLog.setCommon_schema_type("SSL");
                            setSSL_JCZB(ssl_jczb, sessionRecordLog);
                            SSLList.add(ssl_jczb);
                            count++;
                            if (SSLList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList)));
                                SSLList.clear();
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
                            GenericRecord wygjc_jczb = new GenericData.Record(wygjcSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setWYGJC_JCZB(wygjc_jczb, sessionRecordLog);
                            WYGJCList.add(wygjc_jczb);
                            count++;
                            if (WYGJCList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_WYGJC_STREAMID, new Values((Object) generate(wygjcSchema,WYGJCList)));
                                WYGJCList.clear();
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
                            GenericRecord mail_jczb = new GenericData.Record(mailSchema);
                            sessionRecordLog.setCommon_schema_type("MAIL");
                            setMAIL_JCZB(mail_jczb, sessionRecordLog);
                            MAILList.add(mail_jczb);
                            count++;
                            if (MAILList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_MAIL_STREAMID, new Values((Object) generate(mailSchema,MAILList)));
                                MAILList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 508:   //FTP关键词监测
                        try {
                            GenericRecord ftp_jczb = new GenericData.Record(ftpSchema);
                            sessionRecordLog.setCommon_schema_type("FTP");
                            setFTP_JCZB(ftp_jczb,sessionRecordLog);
                            FTPList.add(ftp_jczb);
                            count++;
                            if (MAILList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_FTP_STREAMID, new Values((Object) generate(ftpSchema,MAILList)));
                                FTPList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ftp_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 511:   //VPN监测
                        try {
                            GenericRecord vpn_jczb = new GenericData.Record(vpnSchema);
                            sessionRecordLog.setCommon_schema_type("VPN");
                            setVPN_JCZB(vpn_jczb, sessionRecordLog);
                            VPNList.add(vpn_jczb);
                            count++;
                            if (VPNList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_VPN_STREAMID, new Values((Object) generate(vpnSchema,VPNList)));
                                VPNList.clear();
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
                            GenericRecord jstx_jczb = new GenericData.Record(jstxSchema);
                            sessionRecordLog.setCommon_schema_type("JSTX");
                            setJSTX_JCZB(jstx_jczb, sessionRecordLog);
                            JSTXList.add(jstx_jczb);
                            count++;
                            if (JSTXList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_JSTX_STREAMID, new Values((Object) generate(jstxSchema,VPNList)));
                                JSTXList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------jstx_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 514:   //图片监测
                        try {
                            GenericRecord pic_jczb = new GenericData.Record(picSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setPIC_JCZB(pic_jczb, sessionRecordLog);
                            PICList.add(pic_jczb);
                            count++;
                            if (PICList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_PIC_STREAMID, new Values((Object) generate(picSchema,PICList)));
                                PICList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------pic_jczb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 515:   //多媒体监测
                        try {
                            GenericRecord media_jczb = new GenericData.Record(mediaSchema);
                            sessionRecordLog.setCommon_schema_type("HTTP");
                            setMEDIA_JCZB(media_jczb, sessionRecordLog);
                            MEDIAList.add(media_jczb);
                            count++;
                            if (MEDIAList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.JC_MEDIA_STREAMID, new Values((Object) generate(mediaSchema,MEDIAList)));
                                MEDIAList.clear();
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
                  logger.error("JC数据处理异常"+e);
            }
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_IP_STREAMID,new Fields(StreamAggregateConfig.JC_IP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_DNS_STREAMID,new Fields(StreamAggregateConfig.JC_DNS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_HTTP_STREAMID,new Fields(StreamAggregateConfig.JC_HTTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_SSL_STREAMID,new Fields(StreamAggregateConfig.JC_SSL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_WYGJC_STREAMID,new Fields(StreamAggregateConfig.JC_WYGJC_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MAIL_STREAMID,new Fields(StreamAggregateConfig.JC_MAIL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_FTP_STREAMID,new Fields(StreamAggregateConfig.JC_FTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_VPN_STREAMID,new Fields(StreamAggregateConfig.JC_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_JSTX_STREAMID,new Fields(StreamAggregateConfig.JC_JSTX_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_PIC_STREAMID,new Fields(StreamAggregateConfig.JC_PIC_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MEDIA_STREAMID,new Fields(StreamAggregateConfig.JC_MEDIA_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_COUNT_STREAMID, new Fields("count","loadfail"));
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        Map<String, Object> conf = new HashMap<String, Object>();
        conf.put(org.apache.storm.Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,  StreamAggregateConfig.SAFE_TOPOLOGY_TICK_TUPLE_FREQ_SECS);
        return conf;
    }
}
