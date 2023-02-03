package org.example;


import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.Kafka.KafkaReceiver;
import org.example.functions.Function.writer.CinemaInfoWriter;

import java.io.IOException;
import java.util.List;

@Slf4j
public class KafkaApp {
    public static void main(String[] args) throws IOException, InterruptedException {
        log.info("Start program");

        Config config = ConfigFactory.load("application.conf");

        String masterUrl = config.getString("app.master");
        String appNAme = config.getString("app.name");

        String inputPath = config.getString("app.path.input");
        String outputPath = config.getString("app.path.output");
        String checkPoint = config.getString("app.path.checkpoint");
        List<String> topics = config.getStringList("app.kafka.list");


        SparkSession sparkSession = SparkSession.builder().master(masterUrl).appName(appNAme).getOrCreate();

        FileSystem hdfs = FileSystem.get(sparkSession.sparkContext().hadoopConfiguration());

        JavaStreamingContext javaStreamingContext = JavaStreamingContext.getOrCreate(
                checkPoint,
                ()->{
                    JavaStreamingContext jsc = new JavaStreamingContext(
                            JavaSparkContext.fromSparkContext(sparkSession.sparkContext()),
                            new Duration(1000 * 10)
                    );
                    jsc.checkpoint(checkPoint);


                    KafkaReceiver reciever = new KafkaReceiver (topics,jsc);
                    JavaDStream<CinemaInfo> mensualiteJavaDStream = reciever.get();

                    mensualiteJavaDStream.foreachRDD(
                            mensualiteJavaRDD -> {
                                long ts = System.currentTimeMillis();
                                log.info("batch at {}", ts);
                                Dataset<CinemaInfo> mensualiteDataset = SparkSession.active().createDataset(
                                        mensualiteJavaRDD.rdd(),
                                        Encoders.bean(CinemaInfo.class)
                                ).cache();

                                mensualiteDataset.printSchema();
                                mensualiteDataset.show(5,false);
                                log.info("le nb est {}",mensualiteDataset.count());

                                CinemaInfoWriter<CinemaInfo> writer = new CinemaInfoWriter<>(outputPath +"/time"+ ts);
                                writer.accept(mensualiteDataset);
                                mensualiteDataset.unpersist();
                            }
                    );

                    return jsc;
                },
                sparkSession.sparkContext().hadoopConfiguration()
        );

        javaStreamingContext.start();
        javaStreamingContext.awaitTerminationOrTimeout(1000 * 60 *3);

    }
}
