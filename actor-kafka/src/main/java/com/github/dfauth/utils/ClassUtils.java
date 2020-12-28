package com.github.dfauth.utils;

import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;
import java.util.function.Function;

public class ClassUtils {

    private static final Logger logger = LoggerFactory.getLogger(ClassUtils.class);

    public static <T> T constructFromConfigAndCast(String className, Config config, Function<Object, T> caster) {
        return caster.apply(constructFromConfig(className, config));
    }

    public static Object constructFromConfig(String className, Config config) {
        try {
            return Class.forName(className).getDeclaredConstructor(new Class[]{Config.class}).newInstance(new Object[]{config});
        } catch (InstantiationException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (InvocationTargetException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (NoSuchMethodException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (ClassNotFoundException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
