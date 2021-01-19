package cn.ac.iie.storm.utils.config;

import org.apache.logging.log4j.util.PropertiesUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class GetAppId {
    private static Properties propService = new Properties();
    private static Properties propService1 = new Properties();

    private static Map<String, String> labelMap;
    private static Map<String, String> appId;


    public static String  getJstxDetail_id(String common_app_id) throws IOException {
        return appId.get(common_app_id);
    }

    public static String getProto_id(String common_schema_type) throws IOException {
        return labelMap.get(common_schema_type);
    }

    public static Map<String, String> getLabelMap () throws IOException {

        return labelMap;
    }

    public static Map<String,String> getAPPMap () throws IOException {
        return appId;
    }


    static {
        try {
            InputStream applabel = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_label_coding.properties");
            InputStream appid = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_app_id_coding.properties");

            propService1.load(appid);
            propService.load(applabel);

            appId = new HashMap<String, String>((Map) propService1);
            labelMap = new HashMap<String, String>((Map) propService);
        } catch (Exception e) {
//            propCommon = null;
            propService = null;
            propService1 = null;

        }
    }



    }
