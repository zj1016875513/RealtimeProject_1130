package com.atguigu.gmallpublisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * Created by VULCAN on 2021/4/29
 */
@Data
@AllArgsConstructor
public class Stat {
    String title;
    List<Option> options;
}