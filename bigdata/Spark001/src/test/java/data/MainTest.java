package data;

import com.typesafe.config.ConfigFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.example.Main;
import org.example.functions.Function.reader.HdfsTextFileReader;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit test for simple CollectionsApp.
 */
@Slf4j
public class MainTest {
    String outputPathStr = ConfigFactory.load().getString("app.path.output");

    @Test
    public void test() throws IOException, InterruptedException {
        FileSystem localFs = FileSystem.getLocal(new Configuration());
        log.info("fileSystem used in the test : localFs.getScheme = {}", localFs.getScheme());

        Main.main(new String[0]);
        Path outputPath = new Path(outputPathStr);
        Stream<Path> jsonFilePaths = Arrays.stream(localFs.listStatus(outputPath))
                .map(FileStatus::getPath)
                .filter(p -> p.getName().startsWith("part-") && p.toString().endsWith(".json"));

        List<String> lines = jsonFilePaths
                .flatMap(outputJsonFilePath -> new HdfsTextFileReader(localFs, outputJsonFilePath).get())
                .collect(Collectors.toList());

        assertThat(lines)
                .isNotEmpty()
                .hasSize(1)
                .contains("{\"nom\":\"BALLET\",\"prenoms\":\"YVETTE ALINE\",\"sexe\":2}");
    }
}
