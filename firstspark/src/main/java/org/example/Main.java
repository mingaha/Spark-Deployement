package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;


import lombok.extern.slf4j.Slf4j;

@Slf4j
public class Main {
    public static void main(String[] args) {

        System.out.println("Hello world!");

        Config confifuration = ConfigFactory.load();
        String inputPathStr = confifuration.getString("moi.path.input"); // contient notre repertoire source
        String outputPathStr = confifuration.getString("moi.path.output"); // contient notre repertoire de destination
        String masterUrl = confifuration.getString("moi.spark.master");

        System.out.println( "Hello Spark!" );

        SparkConf sparkConf = new SparkConf().setMaster(masterUrl).setAppName("Main"); // sparkconf permet d'enregistre les configurations de notre  job spark gestionnaire de ressource et le nom de l'appli

        SparkSession sparkSession = new SparkSession.Builder().config(sparkConf).getOrCreate();
        // Limitation de la verbosité
        sparkSession.sparkContext().setLogLevel("WARN");
        //  List<String> jsonData = Arrays.asList(
        //    "{\"Civilite\":\"Nom\",\"Prenom\":{\"Mandat\":\"Circonscription\",\"Departement\":\"Candidat\",\"DatePublication\"}}");
        //Dataset<String> anotherPeopleDataset = sparkSession.createDataset(jsonData, Encoders.STRING());
        // Dataset<Row> anotherPeople = sparkSession.read().json(anotherPeopleDataset);
        // anotherPeople.show();

        Dataset<Row> inputDS = sparkSession.read().option("delimiter",";").option("header", true).csv(inputPathStr);

      //  log.info("nombres lines -> inputDS.count()={} ", inputDS.count());
        inputDS.printSchema();
        inputDS.show(10,false);


      //  inputDS.write().mode(SaveMode.Overwrite).parquet(outputPathStr);

    }
}
