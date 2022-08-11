package cn.ac.iie.storm.utils.combine.httpurl;

import org.apache.log4j.Logger;


public class Http_URL_process {
    private final static Logger logger = Logger.getLogger(Http_URL_process.class);
    public static String  getFile_Name(String url) {
        String name ;
        String file_name = "";
        if (url == null) {
            return null;
        }else if(url.charAt(url.length()-1)=='/'||!url.contains("/")){
            return null;
        }
        else {
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
            if (file_name.contains(".")) {
                return file_name;
            }else {
                return "";
            }

        }}
        }
