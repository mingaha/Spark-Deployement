package org.example.functions.Function;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;
import org.example.functions.Reader.RowToObject;

import java.util.function.Function;

public class Trans1 implements Function<Dataset<Row>, Dataset<CinemaInfo>> {

    private final Row1 parser = new Row1();
    private final MapFunction<Row ,CinemaInfo> SparkParser = parser::apply;
    @Override
    public Dataset<CinemaInfo> apply(Dataset<Row> rowDataset) {
        return   rowDataset.map(SparkParser, Encoders.bean(CinemaInfo.class  ));

    }
}
