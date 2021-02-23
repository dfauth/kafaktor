package com.github.dfauth.utils;

import com.github.dfauth.partial.Unit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

import static com.github.dfauth.partial.Unit.UNIT;

public class TimerUtils {

    private static final Logger logger = LoggerFactory.getLogger(TimerUtils.class);

    public static void timedExecution(Runnable runnable, long d) {
        timedExecution(runnable, Duration.ofMillis(d));
    }

    public static void timedExecution(Runnable runnable, Duration d) {
        try {
            Instant _then = Instant.now();
            runnable.run();
            Thread.sleep(Duration.between(Instant.now(), d.addTo(_then)).toMillis());
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static <T> T timedExecution(Callable<T> callable, long d) {
        return timedExecution(callable, Duration.ofMillis(d));
    }

    public static <T> T timedExecution(Callable<T> callable, Duration d) {
        try {
            Instant _then = Instant.now();
            T result = callable.call();
            Thread.sleep(Duration.between(Instant.now(), d.addTo(_then)).toMillis());
            return result;
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static TimedPayload<Unit> timedExecution(Runnable runnable) {
        try {
            Instant _then = Instant.now();
            runnable.run();
            return create(_then);
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }


    public static <T> TimedPayload<T> timedExecution(Callable<T> callable) {
        try {
            Instant _then = Instant.now();
            return create(_then, callable.call());
        } catch(Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    private static <T> TimedPayload<T> create(Instant _then, T result) {
        Duration elapsed = Duration.between(_then, Instant.now());
        return new TimedPayload<T>() {
            @Override
            public T payload() {
                return result;
            }

            @Override
            public Duration duration() {
                return elapsed;
            }
        };
    }

    private static TimedPayload<Unit> create(Instant _then) {
        Duration elapsed = Duration.between(_then, Instant.now());
        return new TimedPayload<Unit>() {
            @Override
            public Unit payload() {
                return UNIT;
            }

            @Override
            public Duration duration() {
                return elapsed;
            }
        };
    }

    public interface TimedPayload<T> {
        T payload();
        Duration duration();
    }
}
