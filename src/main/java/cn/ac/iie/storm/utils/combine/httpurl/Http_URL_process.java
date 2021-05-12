package cn.ac.iie.storm.utils.combine.httpurl;

import org.apache.log4j.Logger;

import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Http_URL_process {
    private final static Logger logger = Logger.getLogger(Http_URL_process.class);
    public static String  getFile_Name(String url) {
        String name = null;
        String file_name = null;
        if (url == null) {
            return "null";
        } else {
            try {
                String[] ss = url.split("/");
                name = ss[ss.length - 1];
                if (name.contains("?")){
                    String[] split = name.split("\\?");
                    name= split[0];
                }
                if (name.length()>512){
                    file_name = name.substring(0,512);
                }else {
                    file_name = name;
                }
            } catch (ArrayIndexOutOfBoundsException e) {
//            logger.error("该消息中没有File_name.");
            } catch (Exception e) {
                e.printStackTrace();
                System.out.println("这是url" + url);
            }
            return file_name;
        }}
        }
