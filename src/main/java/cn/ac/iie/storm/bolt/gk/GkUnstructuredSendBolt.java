package cn.ac.iie.storm.bolt.gk;


import org.apache.log4j.Logger;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import java.io.IOException;

import static cn.ac.iie.storm.utils.hos.HosService.httpPostObject;
import static cn.ac.iie.storm.utils.http.HttpAsncClient.getConnAnsyHttpPost;

public class GkUnstructuredSendBolt extends BaseBasicBolt {
    String bucketName = null;
    String filename = null;
    String s3Url = null;
    byte[] bytes;
    private final static Logger logger = Logger.getLogger(GkUnstructuredSendBolt.class);
    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {
        try {
            bucketName = tuple.getStringByField("bucketName");
            filename = tuple.getStringByField("filename");
            s3Url = tuple.getStringByField("s3Url");
            bytes = httpPostObject(bucketName, filename);
            if(bytes!=null) {
                getConnAnsyHttpPost(s3Url, bytes);
            }
        } catch (IOException e) {
            logger.error("桶："+bucketName+"下的文件"+filename+"发送失败原因为："+e);
        }
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }
}
