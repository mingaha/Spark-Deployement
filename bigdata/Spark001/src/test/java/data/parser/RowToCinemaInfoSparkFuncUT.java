package data.parser;


import org.example.Beans.CinemaInfo;
import org.example.functions.Function.parser.RowToCinemaInfoSparkFunc;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Java6Assertions.assertThat;

public class RowToCinemaInfoSparkFuncUT {

    @Test
    public void testCall() {
        RowToCinemaInfoSparkFunc f = new RowToCinemaInfoSparkFunc();
        String line = "BALLET*YVETTE ALINE/  21931081301053BOURG-EN-BRESSE2022112101004243";
        /*
        *              address| code|             commune|          nom|num|       region|
|116 AVENUE DES CH...|75108|Paris 8e Arrondis...|UGC NORMANDIE| 31|ILE-DE-FRANCE|
        * */

        CinemaInfo expected = CinemaInfo.builder()
                .num("")
                .nom("")
                .commune("")
                .region("")
                .address("")
                .code("")
                .build();

        CinemaInfo actual = f.call(line);

        assertThat(actual).isEqualTo(expected);
    }
}
