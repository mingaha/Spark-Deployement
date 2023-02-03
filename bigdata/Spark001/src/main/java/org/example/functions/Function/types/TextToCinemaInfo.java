package org.example.functions.Function.types;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.api.java.function.Function;
import org.example.Beans.CinemaInfo;
@Slf4j
public class TextToCinemaInfo implements Function<String, CinemaInfo> {
    @Override
    public CinemaInfo call(String s) throws Exception {


        String[] strings = StringUtils.splitByWholeSeparatorPreserveAllTokens(s,",");

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
