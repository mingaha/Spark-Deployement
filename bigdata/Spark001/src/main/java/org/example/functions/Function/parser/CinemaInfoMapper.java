package org.example.functions.Function.parser;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.example.Beans.CinemaInfo;

import java.util.function.Function;

public class CinemaInfoMapper implements Function<Dataset<String>, Dataset<CinemaInfo>>  {
//    private final RowToCinemaInfoFunc parser = new RowToCinemaInfoFunc();
//    private final MapFunction<Row, CinemaInfo> task = parser::apply;
//    private final MapFunction<Row, CinemaInfo> task = r -> parser.apply(r);
//    private final MapFunction<Row, CinemaInfo> task = new MapFunction<Row, CinemaInfo>() {
//        @Override
//        public CinemaInfo call(Row r) throws Exception {
//            return parser.apply(r);
//        }
//    };

    private final RowToCinemaInfoSparkFunc task = new RowToCinemaInfoSparkFunc();


    @Override
    public Dataset<CinemaInfo> apply(Dataset<String> inputDS) {
        return inputDS.map(task, Encoders.bean(CinemaInfo.class));
    }
}
