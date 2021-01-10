package com.github.dfauth.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;

public class FunctionUtils {

    public static <K,V> BinaryOperator<Map<K,V>> combineMap() {
        return (acc1, acc2) -> {
            HashMap<K, V> tmp = new HashMap<>(acc1);
            tmp.putAll(acc2);
            return tmp;
        };
    }

    public static <K,V> BiFunction<Map<K,V>, Map.Entry<K,V>, Map<K,V>> accumulateMap() {
        return (acc, e) -> {
            HashMap<K, V> tmp = new HashMap<>(acc);
            tmp.put(e.getKey(), e.getValue());
            return tmp;
        };
    }

    public static <T> BinaryOperator<List<T>> combineList() {
        return (acc1, acc2) -> {
            ArrayList<T> tmp = new ArrayList<>(acc1);
            tmp.addAll(acc2);
            return tmp;
        };
    }

    public static <T> BiFunction<List<T>, T, List<T>> accumulateList() {
        return (acc, t) -> {
            acc.add(t);
            return acc;
        };
    }
}
