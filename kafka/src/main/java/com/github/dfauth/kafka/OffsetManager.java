package com.github.dfauth.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;

import static com.github.dfauth.function.Function2.peek;

public interface OffsetManager<K,V> extends KafkaConsumerAware<K,V,TopicPartitionAware.Consumer> {

    class Utils {

        private static final Logger logger = LoggerFactory.getLogger(OffsetManager.class);

        public static <K,V> OffsetManager<K,V> timeBased(Supplier<Instant> s) {
            Instant i = s.get();
            return c -> p -> Optional.ofNullable(c.offsetsForTimes(Map.of(p, i.toEpochMilli())).get(p))
                    .map(peek(o -> {
                        c.seek(p, o.offset());
                        logger.info("time-based offset strategy reset offset to {} for {} using time {}",o.offset(), p, i);
                    })).orElseGet(() -> {
                        logger.info("time-based offset strategy did not find an offset for {} using time {} continue with current offset of {}",p, i, c.position(p));
                        return null;
            });
        }

        public static <K,V> OffsetManager<K,V> seekToStart() {
            return c -> p -> {
                c.seekToBeginning(Collections.singleton(p));
                logger.info("seek to beginning offset strategy reset offset to {} for {}",c.position(p), p);
            };
        }

        public static <K,V> OffsetManager<K,V> current() {
            return c -> p -> {
                logger.info("current offset strategy did not change offset, current offset {} for {}",c.position(p), p);
            };
        }

        public static <K,V> OffsetManager<K,V> seekToEnd() {
            return c -> p -> {
                c.seekToEnd(Collections.singleton(p));
                logger.info("seek to end offset strategy reset offset to {} for {}",c.position(p), p);
            };
        }
    }
}
