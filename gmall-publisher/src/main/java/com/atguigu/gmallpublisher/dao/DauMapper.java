package com.atguigu.gmallpublisher.dao;

import com.atguigu.gmallpublisher.beans.DauPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by VULCAN on 2021/4/24
 */
@Repository
public interface DauMapper {

    //查询每日的日活
    Integer getDauByDate(String date);

    //查询每日的新增设备数
    Integer getNewMidNumsByDate(String date);

    List<DauPerHour> getRealTimeHoursData(String date);


}
