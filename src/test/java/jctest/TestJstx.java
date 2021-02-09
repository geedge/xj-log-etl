package jctest;

import cn.ac.iie.storm.bean.SessionRecordLog;
import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import cn.ac.iie.storm.utils.http.HttpClientService;
import com.alibaba.fastjson.JSONObject;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static cn.ac.iie.storm.utils.general.TransFormUtils.generate;

public class TestJstx {
    public static void main(String[] args) throws IOException {
        List<GenericRecord> JSTXList = new ArrayList<GenericRecord>();
        Schema jstxSchema;
        jstxSchema = new Schema.Parser().parse(StreamAggregateConfig.JSTX_JCZB_SCHEMA);

       // SessionRecordLog sessionRecordLog = JSONObject.parseObject(, SessionRecordLog.class);
        GenericRecord jstx_jczb = new GenericData.Record(jstxSchema);
      //  setJSTX_JCZB(jstx_jczb, sessionRecordLog);
        JSTXList.add(jstx_jczb);
        HttpClientService httpClientService = new HttpClientService();

        Map<String, String> stringStringMap = httpClientService.getConnHttpPost("http://10.111.153.249:10080/", generate(jstxSchema, JSTXList), StreamAggregateConfig.FLOODING_HTTP_POST_SOCKETTIMEOUT, StreamAggregateConfig.JC_POST_JSTX_TOPIC);
        System.out.println(stringStringMap);
        JSTXList.clear();
    }
}
