package org.example.functions.Function.processor;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.writer.CinemaInfoWriter;

@Slf4j
@RequiredArgsConstructor
public class CinemaInfoStreamProcessor implements VoidFunction<JavaRDD<CinemaInfo>> {
    private final String outputPathStr;


    @Override
    public void call(JavaRDD<CinemaInfo> acteDecesJavaRDD) throws Exception {
        long ts = System.currentTimeMillis();
        log.info("micro-batch stored in folder={}", ts);

        if (acteDecesJavaRDD.isEmpty()) {
            log.info("no data found!");
            return;
        }

        log.info("data under processing...");
        final SparkSession sparkSession = SparkSession.active();


        Dataset<CinemaInfo> acteDecesDataset = sparkSession.createDataset(
                acteDecesJavaRDD.rdd(),
                Encoders.bean(CinemaInfo.class)
        );


        acteDecesDataset.printSchema();
        acteDecesDataset.show(5, false);

        log.info("nb actesDeces = {}", acteDecesDataset.count());


        CinemaInfoWriter<CinemaInfo> writer = new CinemaInfoWriter<>(outputPathStr + "/time=" + ts);
        writer.accept(acteDecesDataset);

        acteDecesDataset.unpersist();
        log.info("done");
    }
}
