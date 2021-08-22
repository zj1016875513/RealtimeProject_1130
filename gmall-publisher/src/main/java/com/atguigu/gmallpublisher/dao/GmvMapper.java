package com.atguigu.gmallpublisher.dao;

import com.atguigu.gmallpublisher.beans.DauPerHour;
import com.atguigu.gmallpublisher.beans.GmvPerHour;
import org.springframework.stereotype.Repository;

import java.util.List;

/**
 * Created by VULCAN on 2021/4/26
 */
@Repository // new GmvMapper$
public interface GmvMapper {

    //查询每日的GMV
    Double getGmvByDate(String date);


    //查询每日分时的GMV
    List<GmvPerHour> getRealTimeHoursData(String date);
}
