package cn.acyou.scorpio.es.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author youfang
 * @version [1.0.0, 2020-9-15 下午 10:03]
 **/
@Data
@NoArgsConstructor
@AllArgsConstructor
public class User {

    private String name;

    private Integer age;
}
