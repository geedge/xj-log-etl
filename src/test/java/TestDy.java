import com.amazonaws.services.s3.model.CSVOutput;
import org.apache.logging.log4j.util.PropertiesUtil;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static cn.ac.iie.storm.utils.general.GetMap.getDy;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class TestDy {
    private static Properties propService_city = new Properties();
    InputStream cityNumbering = PropertiesUtil.class.getClassLoader().getResourceAsStream("common_city_coding.properties");
    //        propService_city.load(cityNumbering);
//        Map<String, String> cityMap = new HashMap<String, String>((Map) propService_city);
//        for (String key : cityMap.keySet()) {
//            String str = key.replace(",", ".");
//            String[] split = str.split("\\.");
//            if (split.length != 3) {
//                System.out.println(key);
////                    System.out.println(split[2] + "." + split[1] + "." + split[0] + "=" + cityMap.get(key));
//            }
//        }
    public static void main(String[] args) throws IOException {
        String dy = getDy("China.Fujian.Other");
        System.out.println(dy);
    }
}