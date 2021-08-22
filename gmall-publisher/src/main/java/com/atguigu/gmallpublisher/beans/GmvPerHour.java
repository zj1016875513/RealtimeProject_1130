package com.atguigu.gmallpublisher.beans;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by VULCAN on 2021/4/24
 *
 * "11":383
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class GmvPerHour {

    private String hour;
    private Double gmv;


}
