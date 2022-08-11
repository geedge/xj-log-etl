package cn.ac.iie.storm.bolt.jc;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

import static cn.ac.iie.storm.utils.hos.HosService.httpPostObject;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;
import static cn.ac.iie.storm.utils.http.HttpGetUrl.*;

public class JcUnstructuredSendBolt extends BaseBasicBolt {
    private final static Logger logger = Logger.getLogger(JcUnstructuredSendBolt.class);

    String hosBucketName = null;
    String fileName = null;
    String s3BucketName = null;
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            hosBucketName = tuple.getStringByField("hosBucketName");
            fileName = tuple.getStringByField("fileName");
            s3BucketName = tuple.getStringByField("s3BucketName");
            byte[] bytes = httpPostObject(hosBucketName, fileName);
            int connAnsyHttpPost =0;
            String s3Url;
            if(bytes!=null) {
                int i = 0;
                while (i < StreamAggregateConfig.S3_SENT_COUNT) {
                    s3Url = getS3Url(s3BucketName, fileName );
                    connAnsyHttpPost = getConnAnsyHttpPost(s3Url, bytes);
                    if (connAnsyHttpPost == 200) {
                        break;
                    }
                    i++;
                }
                if (connAnsyHttpPost != 200) {
                    logger.error("桶：" + hosBucketName + "下的文件" + fileName + "发送失败");
                }
            }
            } catch (IOException e) {
            logger.error("桶："+hosBucketName+"下的文件"+fileName+"发送失败原因为："+e);
        }
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
