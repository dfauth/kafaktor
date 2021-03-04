package com.github.dfauth.akka.typed.simple;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import com.typesafe.config.Config;
import scala.Option;

import java.util.Optional;

public abstract class MailboxConverter implements MailboxType {

    protected ActorSystem.Settings settings;
    protected Config config;

    public MailboxConverter(ActorSystem.Settings settings, Config config) {
        this.settings = settings;
        this.config = config;
    }

    @Override
    public final MessageQueue create(Option<ActorRef> optOwner, Option<ActorSystem> system) {
        return create(convert(optOwner), convert(system));
    }

    public static <T> Optional<T> convert(Option<T> o) {
        return o.map(t -> Optional.ofNullable(t)).getOrElse(() -> Optional.empty());
    }

    protected abstract MessageQueue create(Optional<ActorRef> optOwner, Optional<ActorSystem> system);

}
