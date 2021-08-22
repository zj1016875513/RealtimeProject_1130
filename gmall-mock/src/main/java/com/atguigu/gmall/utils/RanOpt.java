package com.atguigu.gmall.utils;

/**
 * Created by VULCAN on 2021/4/21
 */
public class RanOpt<T> {
    T value ;
    int weight;

    public RanOpt ( T value, int weight ){
        this.value=value ;
        this.weight=weight;
    }

    public T getValue() {
        return value;
    }

    public int getWeight() {
        return weight;
    }
}
