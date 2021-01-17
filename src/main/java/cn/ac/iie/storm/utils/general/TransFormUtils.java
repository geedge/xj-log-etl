package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfigurations;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;




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

    /**
     * 获取字节数组的长度
     * @param bytes
     * @return
     */
    public static int getValidLength(byte[] bytes){
        int i = 0;
        if(bytes!=null) {
            for (byte aByte : bytes) {
                i++;
            }
        }
        return i;
    }

    /**
     * 获取运营商
     * @param common_data_center
     * @return
     */
    public static int getYys(String common_data_center) {
        switch (common_data_center) {
            case "CMCC":
                return 1;
            case "CUCC":
                return 2;
            case "CTCC":
                return 3;
            default:return 0;
        }
    }



    public static String  getVpnDetail_id(String common_l7_protocol) throws IOException {

        switch (common_l7_protocol){
            case "PPTP":
                return "04001";
            case "L2TP":
                return "04002";
            case "OPENVPN":
                return "04003";
            case "IPSEC":
                return "04004";
            default:
                return null;
        }
    }

    public static String  getJstxDetail_id(String common_app_id) throws IOException {
        Map<String, String> appId = getAppId();
            return appId.get(common_app_id);
    }



    public static String  getMailDetail_id(String mail_protocol_type) throws IOException {
        switch (mail_protocol_type){
            case "MAIL":
                return "03001";
            case "SMTPS":
                return "03002";
            case "POP3S":
                return "03003";
            case "IMAPS":
                return "03004";
            default:
                return null;
        }
    }







        private static Map getAppId () throws IOException {
            Properties prop = new Properties();
            prop.load(StreamAggregateConfigurations.class.getClassLoader().getResourceAsStream("common_app_id_coding.properties"));
            Map<String, String> Map = new HashMap<String, String>((Map) prop);
            return Map;
        }

        private static Map getAppLabel () throws IOException {
            Properties prop = new Properties();
            prop.load(StreamAggregateConfigurations.class.getClassLoader().getResourceAsStream("common_app_label_coding.properties"));
            Map<String, String> Map = new HashMap<String, String>((Map) prop);
            return Map;
        }


    /**
     * avro对象的序列化方式
     * @param schema
     * @param genericRecordList
     * @return
     */
    public static byte[] generate(Schema schema, List<GenericRecord> genericRecordList){
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            if (genericRecordList != null && genericRecordList.size() > 0) {
                for (int i = 0; i < genericRecordList.size(); i++) {
                    writer.write(genericRecordList.get(i),encoder);
                }
            }
            encoder.flush();
            return out.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }



}

