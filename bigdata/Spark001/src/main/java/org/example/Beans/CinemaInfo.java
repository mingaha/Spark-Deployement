package org.example.Beans;

import lombok.*;
import org.json4s.JsonAST;

import java.io.Serializable;
@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Getter
public class CinemaInfo implements Serializable {

    private String num;
    private String nom;
    private String region;
    private String address;
    private String code;
    private String commune;


}
