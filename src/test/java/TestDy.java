import java.io.IOException;

import static cn.ac.iie.storm.utils.config.GetMap.*;

import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getMailDetail_id;

public class TestDy {
        public static void main(String[] args) throws IOException {
            String sjfws = getSafeFws("2ddw3123@11.com;2ddw3123@qq.com");
            System.out.println(sjfws);

//            String mailRecever = getMailRecever("<2ddw3123@11.com><2ddw3123@11.com><2ddw3123@11.com>");
//            System.out.println(mailRecever);

        }
}