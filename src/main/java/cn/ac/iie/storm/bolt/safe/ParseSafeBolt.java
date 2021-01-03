package cn.ac.iie.storm.bolt.safe;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.bean.Traffic_Forward;
import cn.ac.iie.storm.bean.UserRegion;
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

public class ParseSafeBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(ParseSafeBolt.class);

    /**
     * 管控类
     */
    private SessionRecordLog hmdrzzbSource;
    private SessionRecordLog dnsgkzbSource;
    private SessionRecordLog httpgkzbSource;
    private SessionRecordLog tdzsgkzbSource;
    private SessionRecordLog wygjzgkrzSource;
    private SessionRecordLog mailgkzbSource;
    private SessionRecordLog ftpgjzgkzbSouce;
    private SessionRecordLog vpngkzbSource;
    private SessionRecordLog jstxgkzbSource;

    /**
     * 检测类
     */
    private SessionRecordLog ipjczbSource;
    private SessionRecordLog dnsjczbSource;
    private SessionRecordLog httpjczbSource;
    private SessionRecordLog tdzsjczbSource;
    private SessionRecordLog wygjzjcrzSource;
    private SessionRecordLog mailgjzjczbSource;
    private SessionRecordLog ftpgjzjczbSource;
    private SessionRecordLog vpnjczbSource;
    private SessionRecordLog jstxjczbSource;
    private SessionRecordLog picjczbSource;
    private SessionRecordLog mediajczbSource;
    /**
     * 统计类字段
     */

    private long gkInputLog = 0L;
    private long jcInputLog = 0L;

    public ParseSafeBolt() {
    }

    /**
     * 处理安全数据的逻辑业务
     *
     * @param tuple
     * @param basicOutputCollector
     */
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        //首先获取一条数据
        try {
            String line = tuple.getStringByField("safe-source");
            String jsonMessage = getJsonMessage(line);
            SessionRecordLog sessionRecordLog = JSONObject.parseObject(jsonMessage, SessionRecordLog.class);

            UserRegion userRegion =JSONObject.parseObject(sessionRecordLog.getUserregion(), UserRegion.class);
            Traffic_Forward traffic_forward  =JSONObject.parseObject(userRegion.getTraffic_forward(), Traffic_Forward.class);
            sessionRecordLog.setPzid(traffic_forward.getPzid());
            sessionRecordLog.setFlid(traffic_forward.getFlid());
            sessionRecordLog.setXzid(traffic_forward.getXzid());
            sessionRecordLog.setLhid(traffic_forward.getLhid());
            sessionRecordLog.setPzlx(traffic_forward.getPzlx());
            sessionRecordLog.setGklb(traffic_forward.getGklb());

            int pzlx = traffic_forward.getPzlx();

            //按pzlx的值对数据进行分类发送
            switch(pzlx){
                //管控
                case 201:  //IP黑名单
                case 601:  //网安IP管控
                case 701:  //反诈IP管控
                case 801:  //工业互联网IP管控
                    hmdrzzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 402:   //DNS欺骗
                case 602:   //网安DNS管控
                case 702:   //反诈DNS管控
                case 802:   //工业互联网DNS管控
                    dnsgkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 403:   //URL封堵
                case 404:   //网站封堵
                case 603:   //网安URL管控
                case 703:   //反诈URL管控
                    httpgkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 405:   //特定证书封堵
                    tdzsgkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 406:   //网站关键字封堵
                case 409:   //搜索词封堵
                    wygjzgkrzSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 407:   //邮件关键字封堵
                case 410:
                    mailgkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 408:   //FTP关键字封堵
                    ftpgjzgkzbSouce = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 411:   //VPN封堵
                    vpngkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                case 412:   //特定应用封堵
                case 413:   //社交应用封堵
                    jstxgkzbSource = sessionRecordLog;
                    gkInputLog = line.getBytes().length;
                    break;
                //监测
                case 501:   //IP监测
                    ipjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 502:   //DNS监测
                    dnsjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 503:   //URL监测
                case 504:   //网站监测
                    httpjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 505:   //特定证书监测
                    tdzsjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 506:   //网站关键词监测
                case 509:   //搜索词监测
                    wygjzjcrzSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 507:   //邮件监测
                case 510:   //邮件监测
                    mailgjzjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 508:   //FTP关键词监测
                    ftpgjzjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 511:   //VPN监测
                    vpnjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 512:   //特定应用监测
                case 513:   //社交应用监测
                    jstxjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 514:   //图片监测
                    picjczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
                case 515:   //多媒体监测
                    mediajczbSource = sessionRecordLog;
                    jcInputLog = line.getBytes().length;
                    break;
            }


            //管控类
            basicOutputCollector.emit(StreamAggregateConfig.GK_IP_STREAMID,new Values(hmdrzzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_DNS_STREAMID,new Values(dnsgkzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_HTTP_STREAMID,new Values(httpgkzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_TDZS_STREAMID,new Values(tdzsgkzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_WYGJZ_STREAMID,new Values(wygjzgkrzSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_MAIL_STREAMID,new Values(mailgkzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_FTP_STREAMID,new Values(ftpgjzgkzbSouce));
            basicOutputCollector.emit(StreamAggregateConfig.GK_VPN_STREAMID,new Values(vpngkzbSource));
            basicOutputCollector.emit(StreamAggregateConfig.GK_JSTX_STREAMID,new Values(jstxgkzbSource));

            //检测类JC
            basicOutputCollector.emit(StreamAggregateConfig.JC_IP_STREAMID,new Values(ipjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_DNS_STREAMID,new Values(dnsjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_HTTP_STREAMID,new Values(httpjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_TDZS_STREAMID,new Values(tdzsjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_WYGJZ_STREAMID,new Values(wygjzjcrzSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_MAIL_STREAMID,new Values(mailgjzjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_FTP_STREAMID,new Values(ftpgjzjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_VPN_STREAMID,new Values(vpnjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_JSTX_STREAMID,new Values(jstxjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_PIC_STREAMID,new Values(picjczbSource));
            basicOutputCollector.emit(StreamAggregateConfig.JC_MEDIA_STREAMID,new Values(mediajczbSource));

            //count

        } catch (Exception e) {
            logger.error("安全日志分类分发消息处理异常"+e);
        }
    }
    @Override
    public void declareOutputFields (OutputFieldsDeclarer outputFieldsDeclarer){

        //管控类
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_IP_STREAMID,new Fields(StreamAggregateConfig.GK_IP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_DNS_STREAMID,new Fields(StreamAggregateConfig.GK_DNS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_HTTP_STREAMID,new Fields(StreamAggregateConfig.GK_HTTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_TDZS_STREAMID,new Fields(StreamAggregateConfig.GK_TDZS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_WYGJZ_STREAMID,new Fields(StreamAggregateConfig.GK_WYGJZ_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_MAIL_STREAMID,new Fields(StreamAggregateConfig.GK_MAIL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_FTP_STREAMID,new Fields(StreamAggregateConfig.GK_FTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_VPN_STREAMID,new Fields(StreamAggregateConfig.GK_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.GK_JSTX_STREAMID,new Fields(StreamAggregateConfig.GK_JSTX_STREAMID));

        //检测类
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_IP_STREAMID,new Fields(StreamAggregateConfig.JC_IP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_DNS_STREAMID,new Fields(StreamAggregateConfig.JC_DNS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_HTTP_STREAMID,new Fields(StreamAggregateConfig.JC_HTTP_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_TDZS_STREAMID,new Fields(StreamAggregateConfig.JC_TDZS_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_WYGJZ_STREAMID,new Fields(StreamAggregateConfig.JC_WYGJZ_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MAIL_STREAMID,new Fields(StreamAggregateConfig.JC_MAIL_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_FTP_STREAMID,new Fields(StreamAggregateConfig.JC_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_VPN_STREAMID,new Fields(StreamAggregateConfig.JC_VPN_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_JSTX_STREAMID,new Fields(StreamAggregateConfig.JC_JSTX_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_PIC_STREAMID,new Fields(StreamAggregateConfig.JC_PIC_STREAMID));
        outputFieldsDeclarer.declareStream(StreamAggregateConfig.JC_MEDIA_STREAMID,new Fields(StreamAggregateConfig.JC_MEDIA_STREAMID));

        //count

    }
}
