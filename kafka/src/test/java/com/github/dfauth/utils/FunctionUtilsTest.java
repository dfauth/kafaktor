package com.github.dfauth.utils;

import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;

import static com.github.dfauth.utils.FunctionUtils.merge;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class FunctionUtilsTest {

    private static final Logger logger = LoggerFactory.getLogger(FunctionUtilsTest.class);
    private static final String VALUE = "value";
    private static final String KEY = "key";
    private static final String VALUE1 = "value1";
    private static final String KEY1 = "key1";
    private static final Map<String, String> ref = Map.of(KEY, VALUE);

    @Test
    public void testMerge() {
        assertEquals(ref, merge(Collections.emptyMap(), KEY, VALUE));
        Map<String, String> tmp = merge(Collections.emptyMap(), KEY, VALUE);
        assertEquals(merge(ref, Map.of(KEY1, VALUE1)), merge(tmp, KEY1, VALUE1));
    }
}
