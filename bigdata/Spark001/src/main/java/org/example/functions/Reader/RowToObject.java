package org.example.functions.Reader;

import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;

import java.io.Serializable;
import java.util.function.Function;

public class RowToObject implements Function<Row, CinemaInfo>, Serializable {

    @Override
    public CinemaInfo apply(Row row) {
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
}

