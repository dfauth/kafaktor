package com.github.dfauth.actor.kafka.guice.providers;

import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.bootstrap.BootstrapActor;
import com.typesafe.config.Config;
import org.apache.avro.specific.SpecificRecordBase;

import javax.inject.Inject;
import javax.inject.Provider;

public class BootstrapActorProvider<T extends SpecificRecordBase> implements Provider<BootstrapActor> {

    private BootstrapActor actor;
    private boolean started = false;

    @Inject
    public BootstrapActorProvider(Config config, EnvelopeHandlerImpl<T> envelopeHandlerImpl) {
        actor = new BootstrapActor(config, envelopeHandlerImpl.envelopeDeserializer());
    }

    @Override
    public BootstrapActor get() {
        if(!started) {
            actor.start();
            started = true;
        }
        return actor;
    }
}
