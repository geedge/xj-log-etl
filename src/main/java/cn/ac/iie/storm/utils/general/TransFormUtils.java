package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static cn.ac.iie.storm.utils.general.App_Coding.getAppLabel;

/**
 * 转换或补全工具
 *
 */
public class TransFormUtils {
    private static Logger logger = Logger.getLogger(TransFormUtils.class);
    private static Pattern WEB_PATTERN = Pattern.compile("[^\\\\.]+(\\.com\\.cn|\\.net\\.cn|\\.org\\.cn|\\.gov\\.cn|\\.com|\\.net|\\.cn|\\.org|\\.cc|\\.me|\\.tel|\\.mobi|\\.asia|\\.biz|\\.info|\\.name|\\.tv|\\.hk|\\.公司|\\.中国|\\.网络)");
//    private static IpLookup ipLookup = new IpLookup.Builder(false)
//            .loadDataFileV4(StreamAggregateConfig.IP_LIBRARY + "Kazakhstan.mmdb")
//            .loadDataFileV6(StreamAggregateConfig.IP_LIBRARY + "Kazakhstan.mmdb")
//            .loadAsnDataFileV4(StreamAggregateConfig.IP_LIBRARY + "asn_v4.mmdb")
//            .loadAsnDataFileV6(StreamAggregateConfig.IP_LIBRARY + "asn_v6.mmdb")
//            .build();
//    private static ZookeeperUtils zookeeperUtils = new ZookeeperUtils();
//    private static SnowflakeId snowflakeId = new SnowflakeId();

    /**
     * 解析日志
     *
     * @param message 原始日志
     * @return 修正后的日志
     */
    public static String getJsonMessage(String message) {
        SessionRecordLog sessionRecordLog = JSONObject.parseObject(message, SessionRecordLog.class);
        //    String serverIp = sessionRecordLog.getCommon_server_ip();
        //    String clientIp = sessionRecordLog.getCommon_client_ip();
        try {

            //sessionRecordLog.setUid(SnowflakeId.generateId());
            // sessionRecordLog.setCommon_server_location(ipLookup.countryLookup(serverIp));
            //sessionRecordLog.getCommon_client_location(ipLookup.cityLookupDetail(clientIp));
            //sessionRecordLog.setClient_asn(ipLookup.asnLookup(clientIp, true));
            // sessionRecordLog.setServer_asn(ipLookup.asnLookup(serverIp, true));
            // sessionRecordLog.setDomain(getTopDomain(sessionRecordLog.getSni(), sessionRecordLog.getHost()));

            //protocol对应的common_l4_protocol

            sessionRecordLog.setCommon_recv_time(System.currentTimeMillis() / 1000);
//            sessionRecordLog.setSubscribe_id(getSubscribeId(clientIp));
            return JSONObject.toJSONString(sessionRecordLog);
        } catch (Exception e) {
            logger.error("日志解析过程出现异常", e);
            return "";
        }
    }

    /**
     * 有sni通过sni获取域名，有hots根据host获取域名
     *
     * @param sni  sni
     * @param host host
     * @return 顶级域名
     */
    private static String getTopDomain(String sni, String host) {
        if (StringUtil.isNotBlank(sni)) {
            return getDomain(sni);
        } else if (StringUtil.isNotBlank(host)) {
            return getDomain(host);
        } else {
            return "";
        }
    }


    /**
     * 根据url截取顶级域名
     *
     * @param url 网站url
     * @return 顶级域名
     */
    private static String getDomain(String url) {
        try {
            Matcher matcher = WEB_PATTERN.matcher(url);
            if (matcher.find()) {
                return matcher.group();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return "";
    }

    /**
     * TCP协议返回17
     * UDP协议返回6
     * 其他的返回0
     *
     * @param common_l4_protocol
     * @return
     */
    public static int getProtocol(String common_l4_protocol) {
        int Protocol = 0;
        if (common_l4_protocol.contains("TCP")) {
            Protocol = 17;
        } else if (common_l4_protocol.contains("UDP")) {
            Protocol = 6;
        }
        return  Protocol;
    }

    /**
     * 把common_sled_ip转换为网络序列
     *
     * @param ip:common_sled_ip
     * @return
     */
    public static int getQDJIP(String ip) {
        if (ip == null || ip.length() == 0)
            return 0;
        else if (ip.equals("0"))
            return 0;
        int ip10 = 0;
        String[] ss = ip.trim().split("\\.");
        if (ss.length != 4)
            return 0;
        for (int i = 0; i < 4; i++) {
            ip10 |= Integer.parseInt(ss[i]) << ((i) * 8);
        }
        return ip10;
    }

    /**
     * dip/sip 的处理方法
     * 源IP地址，IPv6与IPv4复用字段，YL为4时，表示IPv4地址，存储为数字的十进制字符串，数字为IPv4网络序对应的整型，存在正负；
     * YL为6时，表示IPv6地址，表示方式为16进制以冒号表示方式。
     * //     * @param common_address_type
     * //     * @param common_client_ip
     *
     * @return
     */
    public static String getIP(int common_address_type,String common_client_ip ){
        String ip ;
        if(common_address_type==4){
            int ip10 = 0;
            String[] ss = common_client_ip.trim().split("\\.");
            if (ss.length != 4)
                return "null";
            for (int i = 0; i < 4; i++) {
                ip10 |= Integer.parseInt(ss[i]) << ((i) * 8);
            }
            ip =String.valueOf(ip10);
        }else {
            ip = common_client_ip;
        }
        return ip;
    }
    public static String getProto_id(String common_schema_type) throws IOException {

        Map<String, String> appLabel = getAppLabel();
        return appLabel.get(common_schema_type);
    }


    public static void main(String[] args) throws IOException {

//        String ip = getIP(6, "192.168.2.4");
//        System.out.println(ip);
        System.out.println(System.currentTimeMillis()/1000);
    }
}
