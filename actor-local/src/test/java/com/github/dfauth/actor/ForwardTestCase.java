package com.github.dfauth.actor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.github.dfauth.actor.ForwardTestCase.IntermediateActor.initial;
import static org.junit.Assert.assertEquals;

public class ForwardTestCase {

    private static final Logger logger = LoggerFactory.getLogger(ForwardTestCase.class);
    private static final String REF_MSG = "Hello World";

    @Test
    public void testIt() throws InterruptedException {

        ArrayBlockingQueue<String> q = new ArrayBlockingQueue<>(10);

        ActorRef<String> ref0 = Actor.fromBehaviorFactory(ctx1 -> (EnvelopeConsumer<String>) _e ->
            _e.replyWith(p -> ctx1.id()+" replies with "+p)
        );
        ActorRef<String> ref = Actor.fromBehaviorFactory(initial(ref0));
        ref.tell(Envelope.of(REF_MSG, e -> {
            q.offer(e.payload());
            return CompletableFuture.completedFuture(null);
        }));
        String result = q.poll(1, TimeUnit.SECONDS);
        assertEquals(ref.id()+" replies with "+ref0.id()+" replies with "+REF_MSG, result);
    }

    static class IntermediateActor implements Behavior.Factory<String> {

        private final ActorRef<String> ref;

        IntermediateActor(ActorRef<String> ref) {
            this.ref = ref;
        }

        static IntermediateActor initial(ActorRef<String> ref) {
            return new IntermediateActor(ref);
        }

        public void receive(ActorContext<String> ctx, Envelope<String> e) {
                    ref.ask(e.payload())
                    .thenAccept(reply -> e.replyWith(ignored -> ctx.id()+" replies with "+reply));
        }

        @Override
        public Behavior<String> withActorContext(ActorContext<String> ctx) {
            return (EnvelopeConsumer<String>) e -> receive(ctx, e);
        }
    }
}
