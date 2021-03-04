package com.github.dfauth.akka.typed.simple;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.dispatch.Envelope;
import akka.dispatch.MailboxType;
import akka.dispatch.MessageQueue;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class SimpleMailbox extends MailboxConverter implements MailboxType {

    private static final Logger logger = LoggerFactory.getLogger(SimpleMailbox.class);

    private Map<String,MessageQueue> queues = new HashMap<>();

    public SimpleMailbox(ActorSystem.Settings settings, Config config) {
        super(settings, config);
    }

    @Override
    protected MessageQueue create(Optional<ActorRef> optOwner, Optional<ActorSystem> system) {
        return optOwner
                .map(o -> queues.computeIfAbsent(o.path().name(), k -> createMessageQueue(k)))
                .orElse(createMessageQueue("none"));
    }

    private MessageQueue createMessageQueue(String _owner) {
        BlockingQueue<Envelope> q = new ArrayBlockingQueue<>(10);
        return new MessageQueue() {
            @Override
            public void enqueue(ActorRef receiver, Envelope handle) {
                q.offer(handle);
                logger.info("enqueue: receiver: {}, handle: {} owner: {}", receiver, handle, _owner);
            }

            @Override
            public boolean hasMessages() {
                logger.info("hasMessages {} owner: {}",(q.size()>0),_owner);
                return q.size() > 0;
            }

            @Override
            public int numberOfMessages() {
                logger.info("numberOfMessages {} owner: {}",q.size(), _owner);
                return q.size();
            }

            @Override
            public void cleanUp(ActorRef owner, MessageQueue deadLetters) {
                logger.info("cleanUp owner: {}, deadletters: {}", _owner, deadLetters);
                q.clear();
            }

            @Override
            public Envelope dequeue() {
                logger.info("dequeue peek: {} owner: {}",q.peek(),_owner);
                Optional<Envelope> e = Optional.ofNullable(q.poll());
                e.ifPresent(_e ->
                        logger.info("owner is {} message is {} sender is {}", _owner, _e.message(), _e.sender())
                );
                return e.orElse(null);
            }
        };
    }

}
