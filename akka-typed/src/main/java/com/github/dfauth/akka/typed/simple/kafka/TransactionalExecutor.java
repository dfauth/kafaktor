package com.github.dfauth.akka.typed.simple.kafka;

import akka.dispatch.DispatcherPrerequisites;
import akka.dispatch.ExecutorServiceConfigurator;
import akka.dispatch.ExecutorServiceFactory;
import com.typesafe.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class TransactionalExecutor extends ExecutorServiceConfigurator {

    private static final Logger logger = LoggerFactory.getLogger(TransactionalExecutor.class);

    public TransactionalExecutor(Config config, DispatcherPrerequisites prerequisites) {
        super(config, prerequisites);
    }

    @Override
    public ExecutorServiceFactory createExecutorServiceFactory(String id, ThreadFactory threadFactory) {
        return () -> Executors.newSingleThreadExecutor(r -> new Thread(null, wrap(r), "kafka-executor"));
    }

    private Runnable wrap(Runnable r) {
        return () -> {
            Transaction t = Transaction.Monitor.create();
            try {
                r.run();
                t.commit();
            } catch(Throwable _t){
                t.rollback(_t);
            }
        };
    }

}
