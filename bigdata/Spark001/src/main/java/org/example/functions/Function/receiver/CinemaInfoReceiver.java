package org.example.functions.Function.receiver;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.example.Beans.CinemaInfo;
import org.example.functions.Function.parser.TextToCinemaInfoFunc;
import org.example.functions.Function.types.CinemaInfoFileInputFormat;
import org.example.functions.Function.types.CinemaInfoLongWritable;
import org.example.functions.Function.types.CinemaInfoText;

import java.util.function.Supplier;

@Slf4j
@RequiredArgsConstructor
public class CinemaInfoReceiver implements Supplier<JavaDStream<CinemaInfo>> {
    private final JavaStreamingContext javaStreamingContext;
    private final String inputPathStr;

    private final TextToCinemaInfoFunc textToCinemaInfoFunc = new TextToCinemaInfoFunc();
    private final Function<String, CinemaInfo> mapper = textToCinemaInfoFunc::apply;
    private final Function<Path, Boolean> filter = p -> p.getName().endsWith(".txt");

    @Override
    public JavaDStream<CinemaInfo> get() {
        JavaPairInputDStream<CinemaInfoLongWritable, CinemaInfoText> inputDStream = javaStreamingContext
                .fileStream(
                        inputPathStr,
                        CinemaInfoLongWritable.class,
                        CinemaInfoText.class,
                        CinemaInfoFileInputFormat.class,
                        filter,
                        true
                );
        return inputDStream.map(t -> t._2().toString()).map(mapper);
    }
}
