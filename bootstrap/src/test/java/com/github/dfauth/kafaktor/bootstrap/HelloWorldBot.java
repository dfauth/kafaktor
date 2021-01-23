package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;
import com.github.dfauth.kafaktor.bootstrap.avro.Greet;

public class HelloWorldBot extends AbstractBehavior<HelloWorld.Greeted> {

    public static Behavior.Factory<HelloWorld.Greeted> create(int max) {
        return Behaviors.setup(context -> new HelloWorldBot(context, max));
    }

    private final int max;
    private int greetingCounter;

    private HelloWorldBot(ActorContext<HelloWorld.Greeted> context, int max) {
        super(context);
        this.max = max;
    }

    @Override
    public Behavior<HelloWorld.Greeted> onMessage(Envelope<HelloWorld.Greeted> e) {
        greetingCounter++;
        getContext().getLogger().info("Greeting {} for {}", greetingCounter, e.payload().whom);
        if (greetingCounter == max) {
            return Behaviors.stopped();
        } else {
            e.replyWith(p -> Greet.newBuilder().setWhom(p.whom).build());
            return this;
        }
    }
}