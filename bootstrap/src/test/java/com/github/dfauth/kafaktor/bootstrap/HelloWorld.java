package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;

public class HelloWorld extends AbstractBehavior<HelloWorld.Greet> {

    public static final class Greet {
        public final String whom;
        public final ActorRef<Greeted> replyTo;

        public Greet(String whom, ActorRef<Greeted> replyTo) {
            this.whom = whom;
            this.replyTo = replyTo;
        }
    }

    public static final class Greeted {
        public final String whom;
        public final ActorRef<Greet> from;

        public Greeted(String whom, ActorRef<Greet> from) {
            this.whom = whom;
            this.from = from;
        }
    }

    public static Behavior<Greet> create() {
        return Behaviors.setup(HelloWorld::new);
    }

    private HelloWorld(ActorContext<Greet> context) {
        super(context);
    }

    @Override
    public Behavior<Greet> onMessage(Envelope<Greet> e) {
        getContext().getLogger().info("Hello {}!", e.payload().whom);
        e.payload().replyTo.tell(new Greeted(e.payload().whom, getContext().self()));
        return this;
    }

}