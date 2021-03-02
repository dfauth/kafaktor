package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;
import static com.github.dfauth.utils.FunctionUtils.*;

public interface ConsumerRecordOps {

    default Map<TopicPartition, OffsetAndMetadata> filterCompletedOffsets(Map<TopicPartition, CompletableFuture<OffsetAndMetadata>> offsets) {
        return offsets.entrySet().stream().filter(e -> e.getValue().isDone()).reduce(new HashMap<>(), accumulateMap(e -> e.getKey(), e -> tryCatch(() -> {
            return e.getValue().get();
        })), combineMap());
    }

    default List<OffsetAndMetadata> filterCompletedOffsets(List<CompletableFuture<OffsetAndMetadata>> offsets) {
        return offsets.stream().filter(f -> f.isDone()).reduce(new ArrayList<>(), accumulateList(f -> tryCatch(() -> {
            return f.get();
        })), combineList());
    }

}
