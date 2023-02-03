package org.example.functions.Reader;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.functions.CinemaSession;

import java.util.function.Supplier;

public class Reader implements Supplier<Dataset<Row>> {


    @Override
    public Dataset<Row> get() {


            String inputPath =CinemaSession.input;
            Dataset<Row> ds = CinemaSession.getSpark().read().option("header",true).csv(inputPath);//.option("delimiter",";").csv(inputPath);
            return  ds;

    }
}
