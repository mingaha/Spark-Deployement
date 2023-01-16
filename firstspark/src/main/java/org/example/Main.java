package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;


import lombok.extern.slf4j.Slf4j;
import org.example.Cine.beans.CinemaInfo;
import org.example.Cine.cinema.Count;
import org.example.Cine.function.CinemaStaFunction;
import org.example.Cine.reader.CsvReader;
import org.example.Cine.writer.CsvWriter;

import java.util.concurrent.TimeUnit;

@Slf4j
public class Main {
    public static void main(String[] args) {

        Config config = ConfigFactory.load("app.properties") ;
        String masterUrl = config.getString("master") ;
        String appName = config.getString("appName") ;
        SparkSession spark = SparkSession.builder()
                .master(masterUrl)
                .appName(appName)
                .getOrCreate() ;
        String inputPath = config.getString("app.data.input") ;
        String outputPath = config.getString("app.data.output") ;
        //spark.sparkContext().setLogLevel("WARN");
        CinemaStaFunction prixStatFunction = new  CinemaStaFunction() ;
        CsvReader csvReader = new CsvReader(spark , inputPath) ;
        CsvWriter csvWriter = new CsvWriter(outputPath) ;
        Dataset<CinemaInfo> count= new Count().apply(csvReader.get());
/*
while (1==1){
    log.info("hkshdd");
}*/
       // csvWriter.accept(prixStatFunction.apply(csvReader.get()));

    }
/*
public class f(){
                System.out.println("Hello world!");
    Config config = Conf                                                                                                                                                                                                                                                                                                                    +A1 A   1111111111igFactory.load("application.conf");
    String masterUrl = config.getString("moi.spark.master");
    //   String outputPathStr = config.getString("app_name");

    SparkSession spark = SparkSession
            .builder().
            master(masterUrl).
            appName("main").
            getOrCreate();

    String inputPathStr = config.getString("moi.path.source");

    String outPathStr = config.getString("moi.path.output");

        spark.sparkContext().setLogLevel("WARN");
    Dataset<Row> inputDS = spark.read().option("header",true).csv(inputPathStr);
    //Dataset<String> inputDS2 = SparkSession.read().textFile(inputPathStr);

        inputDS.printSchema();
        inputDS.show();

       /*  Dataset<Row> stats = ds.groupBy("name").agg(count("product").as("nb"), sum("price").as("expense"));
        stats.write().partitionBy("name").parquet(outputPathStr);

        Dataset<Row> InputDS =  sparkSession.read().csv(inputPathStr);

}*/
}


