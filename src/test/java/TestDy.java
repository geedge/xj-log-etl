import java.io.IOException;

import static cn.ac.iie.storm.utils.config.GetMap.*;

import static cn.ac.iie.storm.utils.general.MailTrans.*;
import static cn.ac.iie.storm.utils.general.TransFormUtils.*;

public class TestDy {
        public static void main(String[] args)  {
//            String mail = getSecurityMailDetail_id(410, "0300231", "WEBMAIL");
//            System.out.println(mail);

            String imap = getYjxy(510, "03000", "IMAP");
            System.out.println(imap);
        }
}