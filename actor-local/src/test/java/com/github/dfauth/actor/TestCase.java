package com.github.dfauth.actor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.*;
import java.util.function.Supplier;

import static org.junit.Assert.assertEquals;

public class TestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestCase.class);
    private static final String REF_MSG = "Hello World";

    @Test
    public void testIt() throws InterruptedException, ExecutionException, TimeoutException {

        CompletableFuture<String> msgRef = new CompletableFuture<>();

        ActorRef<String> ref = Actor.fromMessageConsumer(msg -> {
            msgRef.complete(msg);
        });
        ref.tell(REF_MSG);
        String msg = msgRef.get(1, TimeUnit.SECONDS);
        assertEquals(REF_MSG, msg);
    }

    @Test
    public void testMultiple() throws InterruptedException {

        BlockingQueue<String> q = new ArrayBlockingQueue<>(10);

        ActorRef<String> ref = Actor.fromMessageConsumer(msg -> {
            q.offer(msg);
        });
        ref.tell(REF_MSG);
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(REF_MSG, msg);
        }
        String uuid = UUID.randomUUID().toString();
        ref.tell(uuid);
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(uuid, msg);
        }
    }

    @Test
    public void testAsk() throws InterruptedException {

        BlockingQueue<String> q = new ArrayBlockingQueue<>(10);

        ActorRef<String> ref = Actor.fromEnvelopeConsumer(e ->
            e.replyWith(ignored -> "response to "+e.payload())
        );
        ref.tell(Envelope.of(REF_MSG, e -> {
            q.offer(e.payload());
            return CompletableFuture.completedFuture(null);
        }));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals("response to "+REF_MSG, msg);
        }
    }

    @Test
    public void testStateful() throws InterruptedException {

        BlockingQueue<String> q = new ArrayBlockingQueue<>(10);

        Addressable<String> replyTo = e -> {
            q.offer(e.payload());
            return CompletableFuture.completedFuture(null);
        };

        ActorRef<String> ref = Actor.fromBehavior(State.initial());
        ref.tell(Envelope.of(REF_MSG, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(InitialState.class.getSimpleName(), msg);
        }
        ref.tell(Envelope.of(REF_MSG, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(InitialState.class.getSimpleName(), msg);
        }
        ref.tell(Envelope.of(State.CHANGE_MESSAGE, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(InitialState.class.getSimpleName(), msg);
        }
        ref.tell(Envelope.of(REF_MSG, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(IntermediateState.class.getSimpleName(), msg);
        }
        ref.tell(Envelope.of(State.CHANGE_MESSAGE, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(IntermediateState.class.getSimpleName(), msg);
        }
        ref.tell(Envelope.of(REF_MSG, replyTo));
        {
            String msg = q.poll(1, TimeUnit.SECONDS);
            assertEquals(FinalState.class.getSimpleName(), msg);
        }
    }

    interface State extends Behavior<String> {

        String CHANGE_MESSAGE = "change";

        default State ifChange(String msg, Supplier<State>supplier) {
            if(CHANGE_MESSAGE.equals(msg)) {
                return supplier.get();
            } else {
                return this;
            }
        }
        static State initial() {
            return new InitialState();
        }
    }

    static class InitialState implements State {

        @Override
        public Behavior<String> onMessage(Envelope<String> e) {
            try {
                return ifChange(e.payload(), () -> new IntermediateState());
            } finally {
                e.replyWith(ignored -> this.getClass().getSimpleName());
            }
        }
    }

    static class IntermediateState implements State {

        @Override
        public Behavior<String> onMessage(Envelope<String> e) {
            try {
                return ifChange(e.payload(), () -> new FinalState());
            } finally {
                e.replyWith(ignored -> this.getClass().getSimpleName());
            }
        }
    }

    static class FinalState implements EnvelopeConsumer<String>, State {

        @Override
        public void receive(Envelope<String> e) {
            e.replyWith(ignored -> this.getClass().getSimpleName());
        }
    }
}
