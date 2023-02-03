package org.example.functions.Function.parser;



import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.MapFunction;
import org.example.Beans.CinemaInfo;

@Slf4j
public class RowToCinemaInfoSparkFunc implements MapFunction<String, CinemaInfo> {

    @Override
    public CinemaInfo call(String line) {
        log.info("current line = {}", line);

        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(line,",");

        String nAuto = strings[0];
        String nom = strings[1];
        String regionAdministrative= strings[2];
        String  adresse= strings[3];
        String codeInsee= strings[4];
        String commune= strings[5];

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
