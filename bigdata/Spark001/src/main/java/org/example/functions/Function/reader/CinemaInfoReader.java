package org.example.functions.Function.reader;


import lombok.Builder;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.parser.CinemaInfoMapper;

import java.io.IOException;
import java.util.function.Supplier;

@Slf4j
@Builder
public class CinemaInfoReader implements Supplier<Dataset<CinemaInfo>> {
    private SparkSession sparkSession;
    private FileSystem hdfs;
    private String inputPathStr;

    private final CinemaInfoMapper acteDecesMapper = new CinemaInfoMapper();

    @Override
    public Dataset<CinemaInfo> get() {
        try {
            if(hdfs.exists(new Path(inputPathStr))) {
                Dataset<String> rowDataset = sparkSession.read().textFile(inputPathStr);
                rowDataset.printSchema();
                rowDataset.show(5, false);


                return acteDecesMapper.apply(rowDataset);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return sparkSession.emptyDataset(Encoders.bean(CinemaInfo.class));
    }
}
