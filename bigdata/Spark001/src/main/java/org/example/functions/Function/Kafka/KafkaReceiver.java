package org.example.functions.Function.Kafka;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.utils.Java;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.types.TextToCinemaInfo;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class KafkaReceiver implements Supplier<JavaDStream<CinemaInfo>> {

    private final List<String> topics;
    private final JavaStreamingContext jsc;

    Config config = ConfigFactory.load("application.conf");

    private String bs = config.getString("moi.kafaka.bs");
    private String path = config.getString("moi.kafaka.path");
    private String keyDes = config.getString("moi.kafaka.keyDes");
    private String valDes = config.getString("moi.kafaka.valDes");
    private String ids = config.getString("moi.kafaka.ids");
    private String ski = config.getString("moi.kafaka.ski");
    private String autoReset = config.getString("moi.kafaka.autoReset");
    private String earliest = config.getString("moi.kafaka.earliest");


    private final Map<String, Object> kafkaParams = new HashMap<String,Object>(){{
        put(bs,path);
        put(keyDes, StringDeserializer.class);
        put(valDes,StringDeserializer.class);
        put(ids, ski);
        put(autoReset, earliest);
    }};

    private TextToCinemaInfo ttm = new TextToCinemaInfo();
    private Function<String, CinemaInfo> mapper = ttm::call;

    @Override
    public JavaDStream<CinemaInfo> get() {
        JavaInputDStream<ConsumerRecord<String,String>> directStream = KafkaUtils.createDirectStream(
                jsc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topics,kafkaParams)
        );
        return directStream.map(ConsumerRecord::value).map(mapper);

    }
}
