package cn.ac.iie.storm.bolt.gk;


import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;
import java.util.List;

import static cn.ac.iie.storm.utils.hos.HosService.httpPostObject;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class GkUnstructuredSendBolt extends BaseBasicBolt {
    String bucketName = null;
    String filename = null;
    String bucketname = null;
    byte[] bytes;
    private final static Logger logger = Logger.getLogger(GkUnstructuredSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            bucketName = tuple.getStringByField("bucketName");
            filename = tuple.getStringByField("filename");
            bucketname = tuple.getStringByField("bucketname");
            bytes = httpPostObject(bucketName, filename);
            int connAnsyHttpPost =0;
            String s3Url;
            if(bytes!=null) {
                int i = 0;
                String ip;
                while (i < StreamAggregateConfig.S3_SENT_COUNT) {
                    s3Url = getS3Url(bucketname, filename);
                    connAnsyHttpPost = getConnAnsyHttpPost(s3Url, bytes);
                    if (connAnsyHttpPost == 200) {
                        break;
                    }
                    i++;
                }
                if (connAnsyHttpPost != 200) {
                    logger.error("桶：" + bucketName + "下的文件" + filename + "发送失败");
                }
            }
        } catch (IOException e) {
            logger.error("桶："+bucketName+"下的文件"+filename+"发送失败原因为："+e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
