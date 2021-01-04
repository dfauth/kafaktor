package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.bootstrap.BootstrapActor;
import com.github.dfauth.kafka.Stream;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;
import org.checkerframework.checker.units.qual.K;

import javax.inject.Inject;
import javax.inject.Provider;

public class BootstrapActorProvider<T extends SpecificRecordBase> implements Provider<BootstrapActor<T>> {

    private BootstrapActor<T> actor;
    private boolean started = false;

    @Inject
    public BootstrapActorProvider(Config config, Stream.Builder<String, byte[]> streamBuilder, EnvelopeHandlerImpl<T> envelopeHandlerImpl) {
        actor = new BootstrapActor<>(config, streamBuilder, envelopeHandlerImpl);
    }

    @Override
    public BootstrapActor<T> get() {
        if(!started) {
            actor.start();
            started = true;
        }
        return actor;
    }
}
