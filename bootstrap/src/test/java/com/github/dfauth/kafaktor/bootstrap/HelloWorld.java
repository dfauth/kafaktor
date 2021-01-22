package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.actor.*;

public class HelloWorld extends AbstractBehavior<HelloWorld.Greet> {

    public interface Greet {
        String getWhom();
    }

    public static final class Greeted {
        public final String whom;

        public Greeted(String whom) {
            this.whom = whom;
        }
    }

    public static Behavior.Factory<Greet> create() {
        return Behaviors.setup(HelloWorld::new);
    }

    private HelloWorld(ActorContext<Greet> context) {
        super(context);
    }

    @Override
    public Behavior<Greet> onMessage(Envelope<Greet> e) {
        getContext().getLogger().info("Hello {}!", e.payload().getWhom());
        e.replyWith(p -> new Greeted(p.getWhom()));
        return this;
    }

}