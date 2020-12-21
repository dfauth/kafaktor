package com.github.dfauth.actor;

import com.github.dfauth.partial.VoidFunction;
import com.github.dfauth.trycatch.TryCatch;

import java.util.Optional;
import java.util.concurrent.*;

import static com.github.dfauth.actor.Behaviors.Penultimate.penultimate;
import static com.github.dfauth.actor.Validations.anonymousId;
import static com.github.dfauth.actor.Validations.validateId;

public class ActorContextImpl<T> implements ActorImpl<T>, Runnable, ActorContext<T> {

    private static ExecutorService staticExecutor;
    private Behavior<T> behaviour;
    protected final ExecutorService executor;
    private final BlockingQueue<Envelope<T>> q = new ArrayBlockingQueue<>(10);
    private boolean terminate = true;
    private final String id;

    public ActorContextImpl(String id, BehaviorFactory<T> behaviorFactory, ExecutorService executor) {
        validateId(id);
        this.id = id;
        this.behaviour = behaviorFactory.create(this);
        this.executor = executor;
    }

    @Override
    public void start() {
        terminate = false;
        executor.execute(this);
    }

    @Override
    public void stop() {
        terminate = true;
    }

    public static <T> ActorImpl<T> of(BehaviorFactory<T> f) {
        return of(f, defaultExecutor());
    }

    public static <T> ActorImpl<T> of(BehaviorFactory<T> f, ExecutorService executorService) {
        return new ActorContextImpl<>(anonymousId(), f, executorService);
    }

    private static ExecutorService defaultExecutor() {
        if(staticExecutor == null) {
            synchronized (ActorContextImpl.class) {
                if(staticExecutor == null) {
                    staticExecutor = Executors.newSingleThreadExecutor(r -> new Thread(null, r, ActorContextImpl.class.getSimpleName()+"-default-executor"));
                }
            }
        }
        return staticExecutor;
    }

    @Override
    public ActorRef<T> ref() {
        return new ActorRefImpl<T>(this, id, e -> {
            q.offer(e);
            return CompletableFuture.completedFuture(null);
        });
    }

    @Override
    public void run() {
        executor.execute(() ->
            Optional.ofNullable(q.poll())
                    .map(
                            VoidFunction.peek(
                                    t -> TryCatch.tryCatchIgnore(() ->
                                            this.behaviour = this.behaviour.onMessage(t))
                            )
                    )
        );
        if(!terminate) {
            executor.execute(this);
        }
    }

    @Override
    public String id() {
        return id;
    }

    @Override
    public ActorRef<T> self() {
        return new ActorRefImpl<>(this, id, e -> {
            q.offer(e);
            return CompletableFuture.completedFuture(null);
        });
    }

    static class ActorRefImpl<T> implements ActorRef<T> {

        private final Addressable<T> addressable;
        private final ActorContext<T> ctx;
        private final String id;

        public ActorRefImpl(ActorContext<T> ctx, String id,  Addressable<T> addressable) {
            this.ctx = ctx;
            this.id = id;
            this.addressable = addressable;
        }

        @Override
        public CompletableFuture<T> ask(T t) {
            CompletableFuture<T> f = new CompletableFuture<>();
            tell(Envelope.builder(t).withAddressable(Actor.fromBehavior(penultimate(e -> f.complete(e.payload())))).withCorrelationId().build());
            return f;
        }

        @Override
        public String id() {
            return id;
        }

        @Override
        public CompletableFuture<?> tell(Envelope<T> e) {
            return addressable.tell(e);
        }
    }
}
