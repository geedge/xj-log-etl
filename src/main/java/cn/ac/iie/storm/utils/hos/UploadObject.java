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

        /**
         * // endpointip:http://192.168.40.111:8080/hos
         * String endpoint = "endpoint";
         * // hosaccessKeydefaultsecretKeydefault
         * String accessKey = "default";
         * String secretKey = "default";
         * // AmazonS3Client
         * AmazonS3 s3 = AmazonS3ClientBuilder.standard()
         *  .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey,
         * secretKey)))
         *  .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
         *  .withChunkedEncodingDisabled(true)
         *  .build();
         * //
         * s3.createBucket("bucketname");
         * // AmazonS3Client
         * s3.shutdown();
         */

        String endpoint = "http://10.111.72.200:9098/hos/";
        String accessKey = "default";
        String secretKey = "default";
        AmazonS3 s3 = AmazonS3ClientBuilder.standard().
                withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1"))
                .withChunkedEncodingDisabled(true)
                .build();
//        s3.createBucket("test");

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentType("avsc");

        metadata.setHeader("x-hos-meta-message", "message");

        InputStream input = new FileInputStream(new File("src/resources/common_app_id_coding.properties"));
        s3.putObject("test", "bianhao",input,metadata);

        input.close(); // AmazonS3Client
       s3.shutdown();
    }
}
