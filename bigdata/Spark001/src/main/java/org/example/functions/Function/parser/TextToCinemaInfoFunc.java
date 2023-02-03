package org.example.functions.Function.parser;



import org.apache.commons.lang3.StringUtils;
import org.example.Beans.CinemaInfo;

import java.io.Serializable;
import java.util.function.Function;

public class TextToCinemaInfoFunc implements Function<String, CinemaInfo>, Serializable {

    @Override
    public CinemaInfo apply(String line) {

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

       /* String nom = nomPrenoms[0].trim();
        String prenom = nomPrenoms[1].trim().replace("/", "");

        return CinemaInfo.builder()
                .nom(nom)
                .prenoms(prenom)
                .sexe(Integer.parseInt(line.substring(80, 81)))
                .build();
        */
    }
}
