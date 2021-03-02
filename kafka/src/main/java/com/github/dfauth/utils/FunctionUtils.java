package com.github.dfauth.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.BiFunction;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.stream.Stream;

public class FunctionUtils {

    public static <V> List<V> merge(List<V> l, V v) {
        List<V> tmp = new ArrayList<>(l);
        tmp.add(v);
        return tmp;
    }

    public static <K,V> Map<K,V> merge(Map<K,V> m, K k, V v) {
        Map<K, V> tmp = new HashMap<>(m);
        tmp.merge(k,v, (_k, _v) -> v);
        return tmp;
    }

    public static <K,V> Map<K,V> merge(Map<K,V> m, K k, V v, K k1, V v1) {
        return merge(m, immutableEntryOf(k, v), immutableEntryOf(k1, v1));
    }

    public static <K,V> Map<K,V> merge(Map<K,V> m, K k, V v, K k1, V v1, K k2, V v2) {
        return merge(m, immutableEntryOf(k, v), immutableEntryOf(k1, v1), immutableEntryOf(k2, v2));
    }

    private static <K, V> Map.Entry<K, V> immutableEntryOf(K k, V v) {
        return new Map.Entry<>(){
            @Override
            public K getKey() {
                return k;
            }

            @Override
            public V getValue() {
                return v;
            }

            @Override
            public V setValue(V value) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static <K,V> Map<K,V> merge(Map<K,V> m, Map.Entry<K,V>... entries) {
        return Stream.of(entries).reduce(m,
                accumulateMap(),
                combineMap()
        );
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
        return accumulateMap(e -> e.getKey(), e -> e.getValue());
    }

    public static <K,V,J,U> BiFunction<Map<J,U>, Map.Entry<K,V>, Map<J,U>> accumulateMap(Function<Map.Entry<K,V>,J> keyMapper, Function<Map.Entry<K,V>,U> valueMapper) {
        return (acc, e) -> merge(acc, keyMapper.apply(e), valueMapper.apply(e));
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

    public static <T,R> BiFunction<List<R>, T, List<R>> accumulateList(Function<T,R> f) {
        return (acc, t) -> {
            acc.add(f.apply(t));
            return acc;
        };
    }
}
