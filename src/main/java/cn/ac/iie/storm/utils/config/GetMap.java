package cn.ac.iie.storm.utils.config;

import org.apache.logging.log4j.util.PropertiesUtil;

import java.io.IOException;
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
    private static Map<String, String> appId;
    private static Map<String, String> cityMap;
    private static Map<String, String> provinceMap;
    private static Map<String, String> countryMap;
    private static Map<String, String> jstxIdMap;



    public static String  getJstxDetail_id(String common_app_id) throws IOException {
        if (appId.get(common_app_id) == null) {
            if (jstxIdMap.get(common_app_id) != null){
                return jstxIdMap.get(common_app_id);
            }else {
                return "";
            }
        }else {
            return appId.get(common_app_id);
        }
    }

    public static String getProto_id(String common_schema_type) throws IOException {
        return labelMap.get(common_schema_type);
    }



    public static String getDy(String location){
        String str ="";
        if(location==null){
            return "0";
        }else if (location.contains(",,")){
            String[] split = location.split(",,");
            str = split[0];
            if (str==null){
                str = split[1];
                if (str == null){
                    str = "0";
                }
            }
            return str;
        } else {
            str = cityMap.get(location);
            if (str == null) {
                String[] split = location.split(",");
                str = provinceMap.get(split[1].replace(" ", ""));
                if (str == null) {
                    str = countryMap.get(split[2].replace(" ", ""));
                    if (str == null) {
                        str = "0";
                    }
                }
            }
            return str;
        }
    }

    public static String getSafeDy(String location){
        String str ="";
        if(location==null){
            return "0";
        }else if (location.contains(",,")){
            String[] split = location.split(",,");
            str = split[0];

            if (str==null){
                str = split[1];
                if (str == null){
                    str = "0";
                }
            }
            return str;
        } else {
            str = cityMap.get(location);
            if (str == null) {
                String[] split = location.split(",");
                str = provinceMap.get(split[1].replace(" ", ""));
                if (str == null) {
                    str = countryMap.get(split[2].replace(" ", ""));
                    if (str == null) {
                        str = "0";
                    }
                }
            }
            return str;
        }
    }


    public static String getSzd(String location){
        String[] split = location.split(",");
        String str = split[2].replace(" ","");
        String s = countryMap.get(str);
        if(str.equals("China")){
            return String.valueOf(1);
        }else {
            if (s==null){
                return String.valueOf(3);
            }else {
                return String.valueOf(2);
            }
        }
    }


    static {
        try {
            InputStream applabel = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_label_coding.properties");
            InputStream appid = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_id_coding.properties");
            InputStream jstxid = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_jstx_id_coding.properties");

            InputStream cityNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_city_coding.properties");
            InputStream provinceNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_province_coding.properties");
            InputStream countryNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_country_coding.properties");


            propService1.load(appid);
            propService.load(applabel);
            propService_jstx.load(jstxid);

            propService_city.load(cityNumbering);
            propService_province.load(provinceNumbering);
            propService_country.load(countryNumbering);


            appId = new HashMap<String, String>((Map) propService1);
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
