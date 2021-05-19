package cn.ac.iie.storm.utils.config;

import org.apache.logging.log4j.util.PropertiesUtil;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GetMap {
    private static Properties propService = new Properties();
    private static Properties propService1 = new Properties();


    private static Properties propService_city = new Properties();
    private static Properties propService_province = new Properties();
    private static Properties propService_country = new Properties();
    private static Properties propService_jstx = new Properties();


    private static Map<String, String> labelMap;
    private static Map<String, String> appMap;
    private static Map<String, String> cityMap;
    private static Map<String, String> provinceMap;
    private static Map<String, String> countryMap;
    private static Map<String, String> jstxIdMap;



    public static String  getJstxDetail_id(int common_app_id){
        String appId = String.valueOf(common_app_id);
        if (appMap.get(appId) != null) {
            return appMap.get(appId);
        } else {
            return appId;
        }
    }

    public static String getProto_id(String common_schema_type){
        return labelMap.get(common_schema_type);
    }

    public static String getDy(String location){
        String[] split;
        if(location==null){
            return "0";
        } else if (location.contains(",,")){
           split = location.split(",,");
            if (split.length == 2) {
                String s = split[0];
                if (s.length()<1) {
                    s = split[1];
                    if (s == null) {
                        s = "0";
                    }
                }
                return s;
            }else {
                return "0";
            }
        } else {
            String loc = location.replace(" ","");
            String str = cityMap.get(loc);
            split = loc.split(",");
            if ((str == null&&split.length==3)) {
                str = provinceMap.get(split[1]);
                if (str == null) {
                    String ss = split[2];
                    str = countryMap.get(ss);
                    if (str == null) {
                        str = "0";
                    }
                }
                return str;
            }else if(str !=null){
                return str;
            }else {
                return "0";
            }
        }
    }


    public static String getFjSzd(String location,String mail_from,String mail_protocol_type) {
        if (mail_from != null&& mail_protocol_type.equals("SMTP")) {
            String[] split;
            if (location == null) {
                return String.valueOf(3);
            } else if (location.contains(",,")) {
                return String.valueOf(1);
            } else {
                split = location.split(",");
                if (split.length == 3) {
                    String country = split[2].replace(" ", "");
                    String s = countryMap.get(country);
                    if (country.equals("China")) {
                        return String.valueOf(1);
                    } else if (s != null) {
                        return String.valueOf(2);
                    } else {
                        return String.valueOf(3);
                    }
                } else {
                    return String.valueOf(3);
                }
            }
        }else {
            return String.valueOf(3);
        }
    }


    public static String getSjSzd(String location,String mail_to,String mail_protocol_type) {
        if (mail_to != null&&(mail_protocol_type.equals("POP3")||mail_protocol_type.equals("IMAP"))) {
            String[] split;
            if (location == null) {
                return String.valueOf(3);
            } else if (location.contains(",,")) {
                return String.valueOf(1);
            } else {
                split = location.split(",");
                if (split.length == 3) {
                    String country = split[2].replace(" ", "");
                    String s = countryMap.get(country);
                    if (country.equals("China")) {
                        return String.valueOf(1);
                    } else if (s != null) {
                        return String.valueOf(2);
                    } else {
                        return String.valueOf(3);
                    }
                } else {
                    return String.valueOf(3);
                }
            }
        }else {
            return String.valueOf(3);
        }
    }



    public static String getDns_name(String dns_qname){
        if (dns_qname==null){
            return "";
        }else if (dns_qname.contains(" ")){
            return "";
        }else {
            return dns_qname;
        }
    }


    static {
        try {
            InputStream applabel = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_label_coding.properties");
            InputStream app = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_id_coding.properties");
            InputStream jstxid = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_jstx_id_coding.properties");

            InputStream cityNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_city_coding.properties");
            InputStream provinceNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_province_coding.properties");
            InputStream countryNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_country_coding.properties");


            propService1.load(app);
            propService.load(applabel);
            propService_jstx.load(jstxid);

            propService_city.load(cityNumbering);
            propService_province.load(provinceNumbering);
            propService_country.load(countryNumbering);


            appMap = new HashMap<String, String>((Map)propService1);
            labelMap = new HashMap<String, String>((Map) propService);
            jstxIdMap = new HashMap<String, String>((Map) propService_jstx);
            cityMap = new HashMap<String, String>((Map) propService_city);
            provinceMap = new HashMap<String, String>((Map) propService_province);
            countryMap = new HashMap<String, String>((Map) propService_country);
        } catch (Exception e) {
//            propCommon = null;
            propService = null;
            propService1 = null;
            propService_jstx = null;
            propService_city = null;
            propService_province = null;
            propService_country = null;
        }
    }


}
