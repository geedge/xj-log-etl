package cn.ac.iie.storm.utils.hos;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class UploadObject {
    public static void main(String[] args) throws IOException {

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

        metadata.setHeader("x-hos-meta-message", "message");

        InputStream input = new FileInputStream(new File("src/resources/common_app_id_coding.properties"));
        s3.putObject("test218", "appid",input,metadata);

        input.close(); // AmazonS3Client
       s3.shutdown();
    }
}
