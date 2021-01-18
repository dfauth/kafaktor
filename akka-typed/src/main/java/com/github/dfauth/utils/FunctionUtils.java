package com.github.dfauth.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;

public class FunctionUtils {

    public static <K,V> Map<K,V> merge(Map<K,V> m, K k, V v) {
        Map<K, V> tmp = new HashMap<>(m);
        tmp.merge(k, v, (_k,_v) -> v);
        return tmp;
    }

    public static <K,V> Map<K,V> merge(Map<K,V>... maps) {
        return Stream.of(maps).flatMap(m -> m.entrySet().stream()).reduce(new HashMap<>(),
                accumulateMap(),
                combineMap()
                );
    }

    public static <K,V> BinaryOperator<Map<K,V>> combineMap() {
        return (acc1, acc2) -> {
            HashMap<K, V> tmp = new HashMap<>(acc1);
            tmp.putAll(acc2);
            return tmp;
        };
    }

    public static <K,V> BiFunction<Map<K,V>, Map.Entry<K,V>, Map<K,V>> accumulateMap() {
        return (acc, e) -> merge(acc, e.getKey(), e.getValue());
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
