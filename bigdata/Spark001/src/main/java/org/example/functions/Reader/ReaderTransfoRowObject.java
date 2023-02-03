package org.example.functions.Reader;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;

import java.util.function.Function;

public class ReaderTransfoRowObject implements Function<Dataset<Row>, Dataset<CinemaInfo>> {

    private final RowToObject parser = new RowToObject();
    private final MapFunction<Row ,CinemaInfo> SparkParser = parser::apply;
    @Override
    public Dataset<CinemaInfo> apply(Dataset<Row> rowDataset) {
        return   rowDataset.map(SparkParser, Encoders.bean(CinemaInfo.class  ));

    }
    /*
    public CinemaInfo row (Row row) {
        // String rowValue= row.getString(0); // get avec idex ou la possition
        String nAuto = row.getAs("N° auto");
        String nom = row.getAs("nom");
        String regionAdministrative= row.getAs("région administrative");
        String  adresse= row.getAs("adresse");
        String codeInsee= row.getAs("code INSEE");
        String commune= row.getAs("commune");

        return  CinemaInfo.builder()
                .num(nAuto)
                .nom(nom)
                .commune(commune)
                .region(regionAdministrative)
                .address(adresse)
                .code(codeInsee)
                .build();
    }
*/





}
