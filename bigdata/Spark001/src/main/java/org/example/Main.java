package org.example;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.Groupby;
import org.example.functions.Reader.Reader;
import org.example.functions.Reader.ReaderTransfoRowObject;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

@Slf4j
public class Main {
    public static void main(String[] args) {
        Config config = ConfigFactory.load("configuratuion.conf");

      /*  System.out.println("Hello world!");
        Config config = ConfigFactory.load("configuratuion.conf");
        String masterurl= config.getString("moi.spark.master");
        String appName= config.getString("moi.appName");
        SparkSession spark = SparkSession.builder().master(masterurl).appName(appName).getOrCreate();*/
      /* Connexion spark = new Connexion();
        String inputPath =spark.input;
        String outputPath =spark.output;

        Dataset<Row> ds = spark.GetConnexion().read().option("header",true).csv(inputPath);//.option("delimiter",";").csv(inputPath);
        */
        Reader reader= new Reader();
         reader.get().show(5,true);
        ReaderTransfoRowObject readerTransfoRowObject =new ReaderTransfoRowObject();
       var transform = readerTransfoRowObject.apply(reader.get());
        transform.show(2, true);

       // Dataset<Row> groupBy = getRowDataset(transform);
        //Dataset<CinemaInfo> groupBy = new Groupby().getRowDataset(transform);
        //groupBy.show(5,true);
        //  groupBy.write().partitionBy("nom").parquet(outputPath);




    }
/*
    private static Dataset<Row> getRowDataset(Dataset<CinemaInfo> transform) {
        Dataset<Row> groupBy = transform.groupBy("Nom").agg(count("nom").as("région administrative"),sum("num").as("N° auto"));
        return groupBy;
    }*/
}