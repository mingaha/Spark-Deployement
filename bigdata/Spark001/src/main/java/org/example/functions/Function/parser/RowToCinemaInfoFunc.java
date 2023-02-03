package org.example.functions.Function.parser;


import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;

import java.io.Serializable;
import java.util.function.Function;

public class RowToCinemaInfoFunc implements Function<Row, CinemaInfo>, Serializable {

    private final TextToCinemaInfoFunc textToCinemaInfoFunc = new TextToCinemaInfoFunc();
    @Override
    public CinemaInfo apply(Row row) {
        String line = row.getAs("value") ;
        return textToCinemaInfoFunc.apply(line);
    }
}
