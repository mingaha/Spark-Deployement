package data.parser;//package ca.aretex.irex.explor.data.deces.functions.parser;
//
//import ca.aretex.irex.explor.data.deces.beans.ActeDeces;
//import lombok.extern.slf4j.Slf4j;
//import org.apache.spark.sql.Dataset;
//import org.apache.spark.sql.Encoders;
//import org.apache.spark.sql.Row;
//import org.apache.spark.sql.SparkSession;
//import org.junit.jupiter.api.AfterAll;
//import uk.co.gresearch.spark.diff.Diff;
//import org.junit.jupiter.api.BeforeAll;
//import org.junit.jupiter.api.Test;
//
//import java.util.List;
//
//import static org.assertj.core.api.Assertions.assertThat;
//
//@Slf4j
//public class ActeDecesMapperTest {
//
//    private static SparkSession spark;
//
//    @BeforeAll
//    public static void setUpAll(){
//        log.info("initializing sparkSession for ActeDecesMapperTest ...");
//        spark = SparkSession.builder()
//                .master("local[2]")
//                .appName("ActeDecesMapperUT")
//                .getOrCreate();
//    }
//
//    @Test
//    public void testApply() {
//        log.info("running test on ActeDecesMapper.apply() ...");
//        ActeDecesMapper mapper = new ActeDecesMapper();
//        Dataset<String> lines = spark.createDataset(
//                List.of(
//                        "BALLET*YVETTE ALINE/                                                            21931081301053BOURG-EN-BRESSE                                             2022112101004243                            "
//                ),
//                Encoders.STRING()
//        );
//
//        Dataset<ActeDeces> expected = spark.createDataset(
//                List.of(ActeDeces.builder().nom("BALLET").prenoms("YVETTE ALINE").sexe(2).build()),
//                Encoders.bean(ActeDeces.class)
//        );
//
//        Dataset<ActeDeces> actual = mapper.apply(lines).cache();
//
//        log.info("showing diff dataset ...");
//        Dataset<Row> diff = Diff.of(actual, expected);
//        diff.printSchema();
//        diff.show();
//
//        log.info("showing diffCount dataset ...");
//        Dataset<Row> diffCount = diff.groupBy("diff").count();
//        diffCount.printSchema();
//        diffCount.show();
//
//        log.info("showing diffCountChange dataset ...");
//        Dataset<Row> diffCountChange = diffCount.filter("diff != 'N'");
//        diffCountChange.printSchema();
//        diffCountChange.show();
//
//        //assertThat(diffCountChange.count()).isEqualTo(0);
//        assertThat(diffCountChange.isEmpty()).isTrue();
//        actual.unpersist();
//    }
//
//    @AfterAll
//    public static void tearDownAll(){
//        if(spark != null){
//            spark.stop();
//        }
//    }
//}
