package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import com.github.dfauth.kafaktor.bootstrap.avro.Greet;

import java.util.Optional;
import java.util.concurrent.CompletableFuture;

public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.SayHello> {

    public interface SayHello {
        String getName();
    }

    public static Behavior.Factory<SayHello> create() {
        return Behaviors.setup(HelloWorldMain::new);
    }

    private final ActorRef<HelloWorld.Greet> greeter;

    private HelloWorldMain(ActorContext<SayHello> context) {
        super(context);
        greeter = context.spawn(HelloWorld.create(), "HelloWorld.Greet");
    }

    @Override
    public Behavior<SayHello> onMessage(Envelope<SayHello> e) {
        getContext().spawn(HelloWorldBot.create(3), e.payload().getName());
        CompletableFuture<HelloWorld.Greet> f = greeter.tell(Greet.newBuilder().setWhom(e.payload().getName()).build());
        f.handle((_payload,_exception) -> {
            Optional.ofNullable(_payload).ifPresent(_e ->
                    getContext().getLogger().info("published payload {}",_payload));
            Optional.ofNullable(_exception).ifPresent(_e ->
                    getContext().getLogger().error(_e.getMessage(), _e));
            return HelloWorldMain.this;
        });
        return this;
    }

}