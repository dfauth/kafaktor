package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;

public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.SayHello> {

    interface SayHello {
        String getName();
    }

    public static Behavior.Factory<SayHello> create() {
        return Behaviors.setup(HelloWorldMain::new);
    }

    private final ActorRef<HelloWorld.Greet> greeter;

    private HelloWorldMain(ActorContext<SayHello> context) {
        super(context);
        greeter = context.spawn(HelloWorld.create(), "greeter");
    }

    @Override
    public Behavior<SayHello> onMessage(Envelope<SayHello> e) {
        ActorRef<HelloWorld.Greeted> replyTo =
                getContext().spawn(HelloWorldBot.create(3), e.payload().getName());
        greeter.tell(new HelloWorld.Greet(e.payload().getName(), replyTo));
        return this;
    }

}