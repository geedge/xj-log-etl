package cn.ac.iie.storm.utils.hos;
import org.apache.log4j.Logger;

import static cn.ac.iie.storm.utils.hos.HosService.httpPostObject;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.getS3Url;

public class Send2S3 {
    private final static Logger logger = Logger.getLogger(Send2S3.class);
    public static String SendObject2S3(String url,String s3BucketName) {
        String bucketName = null;
        String filename = null;
        String s3Url = null;
        try {
            String[] split = url.split("/");
            bucketName = split[split.length-2];
            filename = split[split.length-1];
            byte[] bytes = httpPostObject(bucketName, filename);

            s3Url = getS3Url(s3BucketName, filename);
            getConnAnsyHttpPost(s3Url, bytes);

        } catch (Exception e) {
            logger.error("桶："+bucketName+"下的文件"+filename+"发送失败原因为："+e);
        }
        return s3Url;
    }
}
