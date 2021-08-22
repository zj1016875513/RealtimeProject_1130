package com.atguigu.gmall.utils;

import java.util.Random;

/**
 * Created by VULCAN on 2021/4/21
 */
public class RandomNum {
    public static int getRandInt(int fromNum,int toNum){
        return fromNum + new Random().nextInt(toNum-fromNum+1);
    }
}
