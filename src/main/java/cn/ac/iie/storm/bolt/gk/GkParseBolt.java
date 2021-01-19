package cn.ac.iie.storm.bolt.gk;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.bean.Traffic_Forward;
import cn.ac.iie.storm.bean.UserRegion;
import cn.ac.iie.storm.bolt.flooding.ConnParseBolt;
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
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.Assignment.*;

import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getValidLength;

public class GkParseBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ConnParseBolt.class);
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



    private long gkInputSize = 0L;
    private long gkOutputSize = 0L;
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
        try {
            String line = tuple.getStringByField(StreamAggregateConfig.SAFE_SPOUT_FIELD);
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
            gkInputSize = line.getBytes().length;

            if (TupleUtils.isTick(tuple) && gkInputSize != 0) {
                 basicOutputCollector.emit(StreamAggregateConfig.GK_COUNT_STREAMID, new Values(gkInputSize,count,loadfail,gkOutputSize));
                try {
                    basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList)));


                    basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_IP_STREAMID, new Values((Object) generate(ipSchema, IPList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_WYGJC_STREAMID, new Values((Object) generate(wygjcSchema, WYGJCList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_JSTX_STREAMID, new Values((Object) generate(jstxSchema, JSTXList)));
                    basicOutputCollector.emit(StreamAggregateConfig.GK_VPN_STREAMID, new Values((Object) generate(vpnSchema, VPNList)));

                    basicOutputCollector.emit(StreamAggregateConfig.GK_MAIL_STREAMID, new Values((Object) generate(mailSchema, MAILList)));

                    VPNList.clear();
                    DNSList.clear();
                    FTPList.clear();
                    MAILList.clear();
                    JSTXList.clear();
                } catch (Exception e) {
                    e.printStackTrace();
                    logger.error("----------批数据发送异常-----------" + e);
                }
                gkInputSize = 0;
                count = 0;
                loadfail = 0;
                gkOutputSize = 0;
            } else {
                int pzlx = traffic_forward.getPzlx();
                //按pzlx的值对数据进行分类发送
                //按pzlx的值对数据进行分类发送
                switch (pzlx) {
                    //管控
                    case 201:  //IP黑名单
                    case 601:  //网安IP管控
                    case 701:  //反诈IP管控
                    case 801:  //工业互联网IP管控
                        try {
                            GenericRecord ip_gkrz = new GenericData.Record(ipSchema);
                            setIP_GKRZ(ip_gkrz, sessionRecordLog);
                            IPList.add(ip_gkrz);
                            count++;
                            System.out.println("这是ip数据"+line);
                            if (IPList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_IP_STREAMID, new Values((Object) generate(ipSchema, IPList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(ipSchema, IPList));
                                IPList.clear();
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
                        try {
                            GenericRecord dns_gkzb = new GenericData.Record(dnsSchema);
                            setDNS_GKZB(dns_gkzb, sessionRecordLog);
                            DNSList.add(dns_gkzb);

                            count++;
                            if (DNSList.size() >= 3) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_DNS_STREAMID, new Values((Object) generate(dnsSchema, DNSList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(dnsSchema, DNSList));
                                DNSList.clear();
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
                        try {
                            GenericRecord http_gkzb = new GenericData.Record(httpSchema);
                            setHTTP_GKZB(http_gkzb, sessionRecordLog);
                            HTTPList.add(http_gkzb);
                            System.out.println("这是url数据"+line);
                            count++;
                            if (HTTPList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_HTTP_STREAMID, new Values((Object) generate(httpSchema, HTTPList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(httpSchema, HTTPList));
                                HTTPList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------http_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 405:   //特定证书封堵
                        try {
                            GenericRecord ssl_gkzb = new GenericData.Record(sslSchema);
                            setSSL_GKZB(ssl_gkzb, sessionRecordLog);
                            SSLList.add(ssl_gkzb);
                            System.out.println("这是ssl数据"+line);
                            count++;
                            if (SSLList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_SSL_STREAMID, new Values((Object) generate(sslSchema, SSLList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(sslSchema, SSLList));
                                SSLList.clear();
                            }

                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------ssl_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }

                        break;
                    case 406:   //网站关键字封堵
                    case 409:   //搜索词封堵
                        try {
                            GenericRecord wygjc_gkzb = new GenericData.Record(wygjcSchema);
                            setWYGJC_GKZB(wygjc_gkzb, sessionRecordLog);
                            WYGJCList.add(wygjc_gkzb);
                            System.out.println("这是wygjc数据"+line);
                            count++;
                            if (WYGJCList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_WYGJC_STREAMID, new Values((Object) generate(wygjcSchema, WYGJCList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(wygjcSchema, WYGJCList));
                                WYGJCList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------wygjc_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 407:   //邮件关键字封堵
                    case 410:
                        try {
                            GenericRecord mail_gkzb = new GenericData.Record(mailSchema);
                            setMAIL_GKZB(mail_gkzb, sessionRecordLog);
                            MAILList.add(mail_gkzb);

                            System.out.println("这是mail数据"+line);
                            count++;
                            if (MAILList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_MAIL_STREAMID, new Values((Object) generate(mailSchema, MAILList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(mailSchema, MAILList));
                                MAILList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }

                        break;
                    case 408:   //FTP关键字封堵
                        try {
                            GenericRecord ftp_gkzb = new GenericData.Record(ftpSchema);
                            setFTP_GKZB(ftp_gkzb, sessionRecordLog);
                            FTPList.add(ftp_gkzb);
                            System.out.println("这是ftp数据"+line);
                            count++;
                            if (FTPList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID, new Values((Object) generate(ftpSchema, FTPList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(ftpSchema, FTPList));
                                FTPList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------mail_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 411:   //VPN封堵
                        try {
                            GenericRecord vpn_gkzb = new GenericData.Record(vpnSchema);
                            setVPN_GKZB(vpn_gkzb, sessionRecordLog);
                            VPNList.add(vpn_gkzb);

                            System.out.println("这是vpn数据"+line);
                            count++;
                            if (VPNList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_VPN_STREAMID, new Values((Object) generate(vpnSchema, VPNList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(vpnSchema, VPNList));
                                VPNList.clear();
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            logger.error("-----------vpn_gkzb数据封装处理异常-----------" + e);
                            loadfail++;
                        }
                        break;
                    case 412:   //特定应用封堵
                    case 413:   //社交应用封堵
                        try {
                            GenericRecord jstx_gkzb = new GenericData.Record(jstxSchema);
                            setJSTX_GKZB(jstx_gkzb, sessionRecordLog);
                            JSTXList.add(jstx_gkzb);
                            System.out.println("这是jstx数据"+line);
                            count++;
                            if (JSTXList.size() >= StreamAggregateConfig.SAFE_BATCH_INTER_NUM) {
                                basicOutputCollector.emit(StreamAggregateConfig.GK_JSTX_STREAMID, new Values((Object) generate(jstxSchema, JSTXList)));
                                gkOutputSize = gkOutputSize + getValidLength(generate(jstxSchema, JSTXList));
                                JSTXList.clear();
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
            }

            } catch(Exception e){
                e.printStackTrace();
            }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_IP_STREAMID,new Fields(StreamAggregateConfig.GK_IP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_DNS_STREAMID,new Fields(StreamAggregateConfig.GK_DNS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_HTTP_STREAMID,new Fields(StreamAggregateConfig.GK_HTTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_SSL_STREAMID,new Fields(StreamAggregateConfig.GK_SSL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_WYGJC_STREAMID,new Fields(StreamAggregateConfig.GK_WYGJC_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_MAIL_STREAMID,new Fields(StreamAggregateConfig.GK_MAIL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_FTP_STREAMID,new Fields(StreamAggregateConfig.GK_FTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_VPN_STREAMID,new Fields(StreamAggregateConfig.GK_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_JSTX_STREAMID,new Fields(StreamAggregateConfig.GK_JSTX_STREAMID));


    }
}
