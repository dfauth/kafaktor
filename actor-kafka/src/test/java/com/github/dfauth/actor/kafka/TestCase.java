package com.github.dfauth.actor.kafka;

import com.github.dfauth.actor.Actor;
import com.github.dfauth.actor.ActorRef;
import com.github.dfauth.actor.MessageConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.junit.Assert.assertEquals;

public class TestCase {

    private static final Logger logger = LoggerFactory.getLogger(TestCase.class);

    private static final String REF_MSG = "Hello World";

    @Test
    public void testIt() throws InterruptedException, ExecutionException, TimeoutException {

        CompletableFuture<String> msgRef = new CompletableFuture<>();

        ActorRef<String> ref = Actor.fromMessageConsumer(new MyMessageConsumer<>(msgRef));
        ref.tell(REF_MSG);
        String msg = msgRef.get(1, TimeUnit.SECONDS);
        assertEquals(REF_MSG, msg);
    }

    static class MyMessageConsumer<T> implements MessageConsumer<T> {

        private final CompletableFuture<T> msgRef;

        public MyMessageConsumer(CompletableFuture<T> msgRef) {
            this.msgRef = msgRef;
        }

        @Override
        public void receiveMessage(T payload) {
            msgRef.complete(payload);
        }
    }
}
