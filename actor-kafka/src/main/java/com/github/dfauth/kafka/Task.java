package com.github.dfauth.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static com.github.dfauth.trycatch.TryCatch.tryCatch;

public class Task<K,V> implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    private final List<ConsumerRecord<K,V>> records;
    private volatile boolean stopped = false;
    private volatile boolean started = false;
    private volatile boolean finished = false;
    private final CompletableFuture<Long> completion = new CompletableFuture<>();
    private final ReentrantLock startStopLock = new ReentrantLock();
    private final AtomicLong currentOffset = new AtomicLong();
    private final Function<ConsumerRecord<K,V>, Long> recordProcessingPredicate;

    public Task(List<ConsumerRecord<K,V>> records, Function<ConsumerRecord<K,V>, Long> recordProcessingPredicate) {
        this.records = records;
        this.recordProcessingPredicate = record -> tryCatch(() -> recordProcessingPredicate.apply(record), e -> record.offset()+1);
    }

    public void run() {
        startStopLock.lock();
        if (stopped){
            return;
        }
        started = true;
        startStopLock.unlock();
        
        records.stream()
                .filter(r -> !stopped)
                .map(recordProcessingPredicate)
                .forEach(offset -> currentOffset.set(offset));

        finished = true;
        completion.complete(currentOffset.get());
    }

    public long getCurrentOffset() {
        return currentOffset.get();
    }

    public void stop() {
        startStopLock.lock();
        this.stopped = true;
        if (!started) {
            finished = true;
            completion.complete(currentOffset.get());
        }
        startStopLock.unlock();
    }

    public long waitForCompletion() {
        try {
            return completion.get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public boolean isFinished() {
        return finished;
    }

}