import cn.ac.iie.storm.utils.config.StreamAggregateConfig;

import java.io.IOException;

import static cn.ac.iie.storm.utils.hos.HosService.httpPostObject;
import static cn.ac.iie.storm.utils.hos.Send2S3.SendObject2S3;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;

public class TestSendObject {
    public static void main(String[] args) throws IOException {
        String s = "http://10.111.143.200:9098/hos/fw_http_plug/http_response_body_36.189.23123123.36.21827-183.201.125.13.80_88136053457070.txt";
        String[] split = s.split("/");
        String BucketName = split[split.length-2];
        String filename = split[split.length-1];
//        byte[] bytes = httpPostObject("http://10.111.143.239:9098/hos/", BucketName, filename);
//        getConnAnsyHttpPost("http://10.111.151.218:8888/t-http-rzzb/20210312.txt?method=put",bytes);


        System.out.println("---------------------------------------------------");


        byte[] bytes =null;

        if (bytes!=null){
            System.out.println(bytes);
        }
        String s3Url = SendObject2S3(s,"t-http-jczb");
        System.out.println(s3Url);

        String s1 = "http_response_body_36.189.23123123.36.21827-183.201.125.13.80_88136053457070.txt";
        String s2 ="http://10.111.151.245:8888/t-http-jczb/";
        System.out.println(s1.length());
        System.out.println(s2.length());

//        String s = "63.23123e+07";
//        Float a = Float.valueOf(s);
//        System.out.println(a);

    }

}
