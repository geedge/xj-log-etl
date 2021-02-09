package cn.ac.iie.storm.utils.general;

import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MailTrans {
    private static Logger logger = Logger.getLogger(MailTrans.class);


    /**
     * 收件人：多个收件人之间用英文‘;’分割
     * @param mail_to
     * @return
     */
    public static String getMailRecever(String mail_to){
        String str = null;
        if(mail_to!=null) {
            Pattern pattern = Pattern.compile("<([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(mail_to);
            while(matcher.find()) {
                int i = 0;
                if (str!=null) {
                    str=str+";";
                }
                str = str + matcher.group(i);
                i = i++;
            }

        }
        return str;
    }

    /**
     *求recever_num;cc_num;
     * @param s
     * @return
     */
    public static int getNum(String s){
        int num = 0;
        if(s!=null) {
            Pattern pattern = Pattern.compile("<([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(s);
            while (matcher.find()) {
               num++;
            }
        }
        return num;
    }


    /**
     * accessory
     * 是否有附件
     * @param mail_attachment_name
     * @return 0是没有  1是有
     */
    public static int getAccessory(String mail_attachment_name){
        if(!(mail_attachment_name==null)){
            return 0;
        }else {
            return 1;
        }
    }

    public static String getFjfws(String mail_from ){
        StringBuilder str = new StringBuilder();
        if(!(mail_from==null)) {
            Pattern pattern = Pattern.compile("@([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(mail_from);
            int i = 0;
            while(matcher.find()) {
                if (str.length() > 0) {
                    str.append(";");
                }
                str.append(matcher.group(i).replace(">", ""));
                i++;
            }
        }
        return str.toString();
    }
}
