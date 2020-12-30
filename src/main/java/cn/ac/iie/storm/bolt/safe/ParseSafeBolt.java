package cn.ac.iie.storm.bolt.safe;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

import static cn.ac.iie.storm.utils.general.TransFormUtils.getJsonMessage;

public class ParseSafeBolt extends BaseBasicBolt {
    @Override
    public void execute(Tuple input, BasicOutputCollector collector) {
//        String line = input.getStringByField("safe-source");

//     //   System.out.println("安全测试:"+line);
//        String jsonMessage = getJsonMessage(line);

/*     SessionRecordLog sessionRecordLog = JSONObject.parseObject(jsonMessage, SessionRecordLog.class);
        System.out.println("已经收到安全数据:"+jsonMessage);*/
    }
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {

    }
}
