package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.utils.config.StreamAggregateConfigurations;

import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * properties作为MAP集合的使用
 */
public class App_Coding {


    public static Map  getAppId() throws IOException {
        Properties prop = new Properties();
        prop.load(StreamAggregateConfigurations.class.getClassLoader().getResourceAsStream("common_app_id_coding.properties"));

        Map<String, String> Map = new HashMap<String, String>((Map) prop);
       return Map;
    }
  public   static Map   getAppLabel() throws IOException {
        Properties prop = new Properties();
        prop.load(StreamAggregateConfigurations.class.getClassLoader().getResourceAsStream("common_app_label_coding.properties"));
        Map<String, String> Map = new HashMap<String, String>((Map) prop);
        return Map;
    }

    public static void main(String[] args) throws IOException {
        Map appLabel = getAppId();
        System.out.println(appLabel);
    }

}
