package com.github.dfauth.akka.typed.simple.kafka;

import akka.actor.ActorRef;
import akka.dispatch.Envelope;
import akka.dispatch.MessageQueue;
import com.github.dfauth.actor.kafka.avro.ActorMessage;
import com.github.dfauth.partial.Tuple2;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;


public class KafkaMessageQueue implements MessageQueue {

    private static final Logger logger = LoggerFactory.getLogger(KafkaMessageQueue.class);
    private static final MessageQueue EMPTY = new EmptyMessageQueue();

    public static MessageQueue empty() {
        return EMPTY;
    }

    private final BlockingQueue<Envelope> queue;

    public KafkaMessageQueue(int capacity) {
        queue = new ArrayBlockingQueue<>(capacity);
    }

    @Override
    public void enqueue(ActorRef receiver, Envelope handle) {
        queue.offer(handle);
    }

    @Override
    public Envelope dequeue() {
        Optional<Envelope> optEnvelope = Optional.of(queue.peek() == null ? null : queue.remove());
        return optEnvelope.map(e -> {
            Tuple2<CompletableFuture<Long>, ConsumerRecord<String, ActorMessage>> t = ((Tuple2) e.message());
            t._1().complete(t._2().offset()+1);
            return Envelope.apply(t._2(), null);
        }).orElse(null);
    }

    @Override
    public int numberOfMessages() {
        return queue.size();
    }

    @Override
    public boolean hasMessages() {
        return !queue.isEmpty();
    }

    @Override
    public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        queue.stream().forEach(e -> deadLetters.enqueue(owner, e));
    }

    private static class EmptyMessageQueue implements MessageQueue {
        @Override
        public void enqueue(ActorRef receiver, Envelope handle) {
        }

        @Override
        public Envelope dequeue() {
            return null;
        }

        @Override
        public int numberOfMessages() {
            return 0;
        }

        @Override
        public boolean hasMessages() {
            return false;
        }

        @Override
        public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
        }
    }
}
