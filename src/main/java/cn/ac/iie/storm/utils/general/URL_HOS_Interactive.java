package cn.ac.iie.storm.utils.general;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import java.io.*;

/**
 * 从HOS系统下载文件
 */
public class URL_HOS_Interactive {
    public static void main(String[] args) throws IOException {

        String endpoint = "http://192.168.44.12:9098/hos";
        String accessKey = "default";
        String secretKey = "default";
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withChunkedEncodingDisabled(true)
                .build();
        S3Object s3Object = s3.getObject("test", "/ceshi");

        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        long contentLength = objectMetadata.getContentLength();

        String filemessage = objectMetadata.getRawMetadataValue("x-hos-meta-message").toString();

        S3ObjectInputStream objectContent = s3Object.getObjectContent();

        BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));

        while(true){
            String line = reader.readLine();
            if(line == null) break;
            System.out.println("\n"+line);
        }

        reader.close();

        s3.shutdown();
    }
}
