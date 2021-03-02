package cn.ac.iie.storm.utils.combine.httpurl;

import org.apache.log4j.Logger;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Http_URL_process {
    private final static Logger logger = Logger.getLogger(Http_URL_process.class);
    public static String  getFile_Name(String url) {


        String file_name = "";
        if (url == null) {
            return "null";
        } else {
            String str = url.replace(" ", "");
            try {
                String[] ss = url.split("/");
                String s = ss[ss.length - 1];
                String[] split = s.split("\\?");
                file_name = split[0];
            } catch (ArrayIndexOutOfBoundsException e) {
//            logger.error("该消息中没有File_name.");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("这是url" + url);
            }
            return file_name;
        }
    }

}
