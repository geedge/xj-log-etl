import nl.bitwalker.useragentutils.Browser;
import nl.bitwalker.useragentutils.OperatingSystem;
import nl.bitwalker.useragentutils.UserAgent;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.config.GetMap.getDy;
import static cn.ac.iie.storm.utils.config.GetMap.getSafeDy;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getSsqy;


public class TestYdy {
    public static void main(String[] args) {
        String dy = getSafeDy("650109,,650440");
        String dy1 = getDy("Jiaxing,Zhejiang,China");
        System.out.println(dy);

        System.out.println("dy1:"+dy1);
    }
}
