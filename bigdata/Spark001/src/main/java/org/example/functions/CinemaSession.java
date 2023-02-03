package org.example.functions;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import lombok.Getter;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.SparkSession;
@Getter
@Slf4j
@UtilityClass
public class CinemaSession {
   static Config config = ConfigFactory.load("configuratuion.conf");
   public final static String input=config.getString("moi.path.input");
   public final static   String output= config.getString("moi.path.output");


    public static SparkSession getSpark(){
        log.info("get spark session");
        String masterurl= config.getString("moi.spark.master");
        String appName= config.getString("moi.appName");
        SparkSession spark = SparkSession.builder().master(masterurl).appName(appName).getOrCreate();
        return  spark;
    }

}
