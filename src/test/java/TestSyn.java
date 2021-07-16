import cn.ac.iie.storm.utils.config.StreamAggregateConfig;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;

import static cn.ac.iie.storm.utils.combine.httpurl.Http_URL_process.getFile_Name;
import static cn.ac.iie.storm.utils.general.MailTrans.getConnFws;
import static cn.ac.iie.storm.utils.general.MailTrans.getSafeFws;
import static cn.ac.iie.storm.utils.general.TransFormUtils.getVpnDetail_id;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class TestSyn {
    public static void main(String[] args) throws IOException {
//        String safeFws = getSafeFws("575214@550.com");
//        System.out.println(safeFws);

//        String connFws = getConnFws("<575214@550.com>");
//        System.out.println(connFws);

        String file_name = getFile_Name("wwww.360dweqewqeoc.com/");
        System.out.println(file_name);
    }
}
