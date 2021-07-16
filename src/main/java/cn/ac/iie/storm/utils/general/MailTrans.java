package cn.ac.iie.storm.utils.general;

import org.apache.log4j.Logger;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MailTrans {
    private static Logger logger = Logger.getLogger(MailTrans.class);


    /**
     * 收件人：多个收件人之间用英文‘;’分割
     *
     * @param mail_to
     * @return
     */
    public static String getMailRecever(String mail_to) {
        String str = "";
        if (mail_to != null) {
//            Pattern pattern = Pattern.compile("/^[a-z]([a-z0-9]*[-_]?[a-z0-9]+)*@([a-z0-9]*[-_]?[a-z0-9]+)+[\\.][a-z]{2,3}([\\.][a-z]{2})?$/i");
            Pattern pattern = Pattern.compile("<([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(mail_to);
            while (matcher.find()) {
                int i = 0;
                if (!str.isEmpty()) {
                    str = str + ";";
                }
                str = str + matcher.group(i).replace(">","").replace("<","");
                i = i++;
            }

        }
        return str;
    }

    /**
     * 求recever_num;cc_num;
     *
     * @param s
     * @return
     */
    public static int getNum(String s) {
        int num = 0;
        if (s != null) {
            Pattern pattern = Pattern.compile("<([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(s);
            while (matcher.find()) {
                num++;
            }
        }
        return num;
    }

    public static int geSecuritytNum(String s) {
        int num = 0;
        if (s != null) {
            String[] split = s.split(";");

            num = split.length;
        }
        return num;
    }


    /**
     * accessory
     * 是否有附件
     *
     * @param mail_attachment_name
     * @return 0是没有  1是有
     */
    public static int getAccessory(String mail_attachment_name) {
        if (mail_attachment_name != null) {
            return 1;
        } else {
            return 0;
        }
    }

    public static String getConnFws(String mail_from) {
        StringBuilder str = new StringBuilder();
        if (!(mail_from == null)) {
            Pattern pattern = Pattern.compile("@([\\s\\S]*?)>");
            Matcher matcher = pattern.matcher(mail_from);
            while (matcher.find()) {
                if (str.length() > 0) {
                    str.append(";");
                }
                str.append(matcher.group().replace(">", "").replace("@",""));
            }
        }
        return str.toString();
    }

    public static String getSafeFws(String mail_from) {
        String s = mail_from+";";
        StringBuilder str = new StringBuilder();
        if (!(mail_from == null)) {
          Pattern pattern = Pattern.compile("@([\\s\\S]*?);");
            Matcher matcher = pattern.matcher(s);
            while (matcher.find()) {
                str.append(matcher.group());
            }
            str.replace(str.length()-1,str.length(),"");
            str.replace(0,1,"");
        }
        return str.toString();
    }

}