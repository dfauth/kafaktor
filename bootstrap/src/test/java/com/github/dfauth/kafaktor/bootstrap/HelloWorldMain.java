package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;

public class HelloWorldMain extends AbstractBehavior<HelloWorldMain.SayHello> {

    public static class SayHello {
        public final String name;

        public SayHello(String name) {
            this.name = name;
        }
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
                getContext().spawn(HelloWorldBot.create(3), e.payload().name);
        greeter.tell(new HelloWorld.Greet(e.payload().name, replyTo));
        return this;
    }

}