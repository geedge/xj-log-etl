package cn.ac.iie.storm.utils.hos;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.log4j.Logger;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URLDecoder;

public class HosService {
    private final static Logger logger = Logger.getLogger(HosService.class);
    public static byte[] httpPostObject(String BucketName,String filename)  {
        String endpoint = "endpoint";
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("default", "default");

        byte[] buffer = null;
        S3ObjectInputStream objectContent = null;
        ByteArrayOutputStream bos = null;
        AmazonS3 s3 = null;
        try {
            s3 = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(StreamAggregateConfig.HOS_URL, "us-east-1"))
                    .withChunkedEncodingDisabled(true)
                    .build();
            String decode = URLDecoder.decode(filename);
        S3Object s3Object = s3.getObject(BucketName, decode);
        objectContent = s3Object.getObjectContent();
        bos = new ByteArrayOutputStream();
        byte[] b = new byte[1024];
        int n;
            while ((n = objectContent.read(b)) != -1) {
                bos.write(b, 0, n);
            }
        objectContent.close();
        buffer = bos.toByteArray();
        bos.close();
       
        } catch (IOException | RuntimeException e) {
            logger.error("获取"+BucketName+"下"+filename+"异常,异常原因为"+e);
        } finally {

                try {
                    if (bos!=null) {
                        bos.close();
                    }
                    if(objectContent!=null){
                        objectContent.close();
                    }
                    if (s3!=null){
                        s3.shutdown();
                    }
                } catch (IOException e) {
                    logger.error("流关闭异常，异常原因为："+e);
                }

        }
        return buffer;
    }
}
