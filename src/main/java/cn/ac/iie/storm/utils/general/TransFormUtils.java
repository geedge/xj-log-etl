package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfigurations;
import com.alibaba.fastjson.JSONObject;
import com.zdjizhi.utils.StringUtil;
import nl.bitwalker.useragentutils.Browser;
import nl.bitwalker.useragentutils.OperatingSystem;
import nl.bitwalker.useragentutils.UserAgent;
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
            Protocol = 6;
        } else if (common_l4_protocol.contains("UDP")) {
            Protocol = 17;
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
                return "";
            for (int i = 0; i < 4; i++) {
                ip10 |= Integer.parseInt(ss[i]) << ((i) * 8);
            }
            ip =String.valueOf(ip10);
        }else {
            ip = common_client_ip;
        }
        return ip;
    }

    /**
     * jc、gk类yjxy处理方式
     * @param yjxy
     * @param mail_protocol_type
     * @return
     */
    public static String getYjxy(int pzlx,String yjxy,String mail_protocol_type) {
        if (pzlx==510||pzlx==410){
        switch (yjxy) {
                case "03001":
                    return "WEBMAIL";
                case "03002":
                    return "SMTP";
                case "03003":
                    return "POP3";
                case "03004":
                    return "IMAP";
                case "03000":
                    return mail_protocol_type;
                default:
                    return null;
            }
        }else {
            return mail_protocol_type;
        }
    }
    /**
     * 获取运营商
     * @param common_data_center
     * @return
     */
    public static int getYys(String common_data_center) {
        int yys;
        switch (common_data_center) {
            case "CMCC":
                yys = 1;
                break;
            case "CUCC":
                yys = 2;
                break;
            case "CTCC":
                yys = 3;
                break;
            default:
                yys = 0;
                break;
        }
         return yys;
    }





    public static String  getVpnDetail_id(String common_l7_protocol,String common_protocol_label){
        if (StringUtil.isNotEmpty(common_protocol_label)) {
            if (common_l7_protocol.equals("PPTP") || common_protocol_label.contains("PPTP")) {
                return "04001";
            } else if (common_l7_protocol.equals("L2TP") || common_protocol_label.contains("L2TP")) {
                return "04002";
            } else if (common_l7_protocol.equals("OPENVPN") || common_protocol_label.contains("OPENVPN")) {
                return "04003";
            } else if (common_l7_protocol.equals("ISAKMP") || common_protocol_label.contains("IPSEC")) {
                return "04004";
            } else {
                return null;
            }
        }else {
            switch (common_l7_protocol) {
                case "PPTP":
                    return "04001";
                case "L2TP":
                    return "04002";
                case "OPENVPN":
                    return "04003";
                case "ISAKMP":
                    return "04004";
                default:
                    return null;
            }
        }
    }





    public static String  getMailDetail_id(String mail_protocol_type) {
        switch (mail_protocol_type){
            case "WEBMAIL":
            case "MAIL":
                return "03001";
            case "SMTP":
                return "03002";
            case "POP3":
                return "03003";
            case "IMAP":
                return "03004";
            default:
                return null;
        }
    }







    /**
     * avro对象的序列化方式
     * @param schema
     * @param genericRecordList
     * @return
     */
    public static byte[] generate(Schema schema, List<GenericRecord> genericRecordList) throws IOException {
        GenericDatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(out, null);
        try {
            if (genericRecordList != null && genericRecordList.size() > 0) {
                for (int i = 0; i < genericRecordList.size(); i++) {
                    writer.write(genericRecordList.get(i), encoder);
                }
            }
            encoder.flush();
            return out.toByteArray();
        } finally {
            try {
                out.close();
            } catch (IOException e) {
                logger.error("序列化流资源释放异常");
            }
        }
    }
}

