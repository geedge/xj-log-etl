package cn.ac.iie.storm.utils.combine.httpurl;

import org.apache.log4j.Logger;

import java.net.URL;

public class Http_URL_process {
    private final static Logger logger = Logger.getLogger(Http_URL_process.class);
    public static String  getFile_Name(String url) {
        String file_name ="";
        String s = "https://" + url;
        try {
           URL  url1 = new URL(s);

            String[] split = url1.getPath().split("/");
            file_name = split[split.length-1];

        } catch (ArrayIndexOutOfBoundsException e) {
//            logger.error("该消息中没有File_name.");
        }
        catch (Exception e) {
           e.printStackTrace();
        }
       return file_name;
    }

    public static void main(String[] args) {
        String file_name = getFile_Name("192.168.2.202/");
        System.out.println(file_name);
    }
}
