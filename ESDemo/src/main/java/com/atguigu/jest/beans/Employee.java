package com.atguigu.jest.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by VULCAN on 2021/4/28
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class Employee {

    private String empid;
    private String name;
    private String gender;
    private String hobby;
    private Double balance;
    private Integer age;
}
