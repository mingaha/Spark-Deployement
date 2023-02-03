package org.example.functions.Reader;

import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.example.Beans.CinemaInfo;
import org.example.functions.CinemaSession;

import java.util.function.Supplier;

public class ReaderRow implements Supplier<Dataset<CinemaInfo>> {

     private FileSystem  hdfs;
    @Override
    public Dataset<CinemaInfo> get() {
        String inputPath =CinemaSession.input;
        Dataset<CinemaInfo> ds =CinemaSession.getSpark().read()
                            .option("header",true)
                            .csv(inputPath)
                            .as(Encoders.bean(CinemaInfo.class));//.option("delimiter",";").csv(inputPath);
        return  ds;

    }

}
