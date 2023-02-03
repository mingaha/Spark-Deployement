package org.example.functions.writer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Dataset;
import org.example.Beans.CinemaInfo;
import org.example.functions.CinemaSession;

import java.util.function.Consumer;
@Slf4j
@RequiredArgsConstructor
public class writer<T> implements Consumer<Dataset<T>> {
    @Override
    public void accept(Dataset<T> tDataset) {
       var spark = CinemaSession.getSpark();
        String outputPath =CinemaSession.output;
        try {
            tDataset.write().csv(outputPath);
        }
        catch (Exception e){
            log.info("ci existe recrite////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////// ");
            tDataset.write().mode("overwrite").csv(outputPath);
        }

    }


}
