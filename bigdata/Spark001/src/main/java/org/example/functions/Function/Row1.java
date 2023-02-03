package org.example.functions.Function;

import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;

import java.io.Serializable;
import java.util.function.Function;

public class Row1 implements Function<Row, CinemaInfo>, Serializable {
    @Override
    public CinemaInfo apply(Row row) {

        // String rowValue= row.getString(0); // get avec idex ou la possition
        String nAuto = row.getAs("N° auto");
        String nom = row.getAs("nom");


        return  CinemaInfo.builder()
                .num(nAuto)
                .nom(nom)
                .build();
    }
}
