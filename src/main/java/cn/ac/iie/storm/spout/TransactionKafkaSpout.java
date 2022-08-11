package cn.ac.iie.storm.spout;

import cn.ac.iie.storm.utils.config.StreamAggregateConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.log4j.Logger;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class TransactionKafkaSpout extends BaseRichSpout {
    private static final long serialVersionUID = -3363788553406229592L;
    private KafkaConsumer<String, String> consumer;
    private SpoutOutputCollector collector = null;
    private TopologyContext context = null;
    private final static Logger logger = Logger.getLogger(ConnectionKafkaSpout.class);

    private static Properties createConsumerConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", StreamAggregateConfig.BOOTSTRAP_SERVERS);
        props.put("group.id", StreamAggregateConfig.GROUP_ID);
        props.put("session.timeout.ms", "60000");
        props.put("max.poll.records", 3000);
        props.put("max.partition.fetch.bytes", 31457280);
        props.put("auto.offset.reset", StreamAggregateConfig.AUTO_OFFSET_RESET);
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        logger.info("链接kafka"+StreamAggregateConfig.BOOTSTRAP_SERVERS+"成功");
        return props;
    }

    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector spoutOutputCollector) {
        this.collector=spoutOutputCollector;
        this.context=topologyContext;
        Properties prop = createConsumerConfig();
        this.consumer = new KafkaConsumer<>(prop);
        this.consumer.subscribe(Collections.singletonList(StreamAggregateConfig.KAFKA_TOPIC_TRANSACTION));
    }
    @Override
    public void close() {
        consumer.close();
    }
    @Override
    public void nextTuple() {
        try {
            ConsumerRecords<String, String> records = consumer.poll(StreamAggregateConfig.KAFKA_COMSUMER_POLL);
            Thread.sleep(StreamAggregateConfig.TOPOLOGY_SPOUT_SLEEP_TIME);
            for (ConsumerRecord<String, String> record : records) {
                this.collector.emit(new Values(record.value()));
            }
        } catch (Exception e) {
            logger.error("KafkaSpout发送消息出现异常!", e);
            e.printStackTrace();
        }
    }


    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
        outputFieldsDeclarer.declare(new Fields("transaction-source"));
    }
}
