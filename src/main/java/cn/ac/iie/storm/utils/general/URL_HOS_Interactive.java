package cn.ac.iie.storm.utils.general;

import cn.ac.iie.storm.utils.http.HttpClientService;
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
/*      //这是上传的测试
        String endpoint = "http://192.168.44.12:9098/hos/";
        String accessKey = "default";
        String secretKey = "default";
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withChunkedEncodingDisabled(true)
                .build();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("avsc");
        metadata.setContentLength(878);
        metadata.setHeader("x-hos-meta-message", "message");

        InputStream input = new FileInputStream(new File("src/main/resources/floodingschema/DNS_RZZB.avsc"));
        s3.putObject("test", "ceshi",input,metadata);

        input.close(); // AmazonS3Client
       s3.shutdown();*/
        String endpoint = "http://192.168.44.12:9098/hos/";
        String accessKey = "default";
        String secretKey = "default";

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1")).withChunkedEncodingDisabled(true).build();

        S3Object s3Object = s3.getObject("test", "ceshi");

        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        long contentLength = objectMetadata.getContentLength();
        String fileMessage = objectMetadata.getRawMetadataValue("x-hos-meta-message").toString();

        S3ObjectInputStream objectContent = s3Object.getObjectContent();

        BufferedReader reader = new BufferedReader(new InputStreamReader(objectContent));
        byte[] bytes = reader.readLine().getBytes();

        String s = bytes.toString();
        System.out.println(s);
/*        HttpClientService httpClientService = new HttpClientService();
        httpClientService.gethttpFloodingPost("http://192.168.40.119:7700",bytes,100);*/
            s3.shutdown();

    }
}
