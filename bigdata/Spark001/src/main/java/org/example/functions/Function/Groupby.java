package org.example.functions.Function;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.example.Beans.CinemaInfo;

import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.sum;

public class Groupby {
    public Dataset<CinemaInfo> getRowDataset(Dataset<CinemaInfo> transform) {
        Dataset<Row> groupBy = transform.groupBy("Nom").agg(count("nom").as("région administrative"),sum("num").as("N° auto"));
        //Dataset<Row> data = ...; // your input dataset
      //  KeyValueGroupedDataset<String, Row> groupedData = transform.groupByKey(row-> row.getAs("key"), Encoders.STRING());

        Trans1 readerTransfoRowObject= new Trans1();
       Dataset<CinemaInfo> resulte= readerTransfoRowObject.apply(groupBy);
       resulte.show();
        return resulte ;

    }
}
