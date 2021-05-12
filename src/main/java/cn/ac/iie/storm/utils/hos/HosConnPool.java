package cn.ac.iie.storm.utils.hos;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class HosConnPool {
    private static ClientConfiguration clientConf = null;
    private static AmazonS3 s3 = null;
    private static HosConnPool hosConnPool = null;

    public AmazonS3 getS3Conn(){
        clientConf = new ClientConfiguration();
        clientConf.setMaxConnections(StreamAggregateConfig.HOS_POOL_MAXCONNECTIONS);
        clientConf.setMaxErrorRetry(StreamAggregateConfig.HOS_POOL_MAXERRORRETRY);
        clientConf.setRequestTimeout(StreamAggregateConfig.HOS_POOL_REQUESTTIMEOUT);
        clientConf.setConnectionTimeout(StreamAggregateConfig.HOS_POOL_CONNECTIONTIMEOUT);
        clientConf.setConnectionTTL(StreamAggregateConfig.HOS_POOL_CONNECTIONTTL);
        clientConf.setSocketTimeout(StreamAggregateConfig.HOS_POOL_SOCKETTIMEOUT);
        BasicAWSCredentials awsCredentials = new BasicAWSCredentials("default", "default");
        s3 = AmazonS3ClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(awsCredentials))
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(StreamAggregateConfig.HOS_URL, "us-east-1"))
                .withChunkedEncodingDisabled(true)
                .withClientConfiguration(clientConf)
                .build();
        return s3;
    }
    public static synchronized HosConnPool getInstance(){
        if(null==hosConnPool){
            hosConnPool = new HosConnPool();
        }
        return hosConnPool;
    }
}
