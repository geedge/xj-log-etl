package cn.ac.iie.storm.utils.hos;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;

import java.io.*;

public class DownloadObject {
    public static void main(String[] args) throws IOException {
        String endpoint = "http://10.111.72.200:9098/hos/";
        String accessKey = "default";
        String secretKey = "default";

        AmazonS3 s3 = AmazonS3ClientBuilder.standard().withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us-east-1")).withChunkedEncodingDisabled(true).build();

        S3Object s3Object = s3.getObject("test", "bianhao");

        ObjectMetadata objectMetadata = s3Object.getObjectMetadata();

        long contentLength = objectMetadata.getContentLength();
        String fileMessage = objectMetadata.getRawMetadataValue("x-hos-meta-message").toString();

        S3ObjectInputStream objectContent = s3Object.getObjectContent();



        byte[] buffer = null;

            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            byte[] b = new byte[1024];
            int n;
            while ((n = objectContent.read(b)) != -1)
            {
                bos.write(b, 0, n);
            }
            objectContent.close();
            bos.close();
            buffer = bos.toByteArray();
            s3.shutdown();

        OutputStream out = new FileOutputStream("src/resources/1.txt");
        ByteArrayInputStream is = new ByteArrayInputStream(buffer);
        byte[] buf = new byte[1024];
        int len = 0;
        while((len=is.read(buf))!=-1){
            out.write(buf, 0, len);
        }
        is.close();
        out.close();
    }

}
