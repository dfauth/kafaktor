package com.github.dfauth.actor.kafka.guice;

import com.github.dfauth.actor.kafka.EnvelopeHandlerImpl;
import com.github.dfauth.actor.kafka.bootstrap.BootstrapActor;
import com.github.dfauth.actor.kafka.guice.providers.BootstrapActorProvider;
import com.github.dfauth.actor.kafka.guice.providers.EnvelopeHandlerProvider;
import com.github.dfauth.actor.kafka.guice.providers.MyConfigProvider;
import com.github.dfauth.actor.kafka.guice.providers.StreamBuilderProvider;
import com.github.dfauth.kafka.StreamBuilder;
import com.github.dfauth.utils.MyConfig;
import com.google.inject.AbstractModule;
import com.google.inject.TypeLiteral;
import org.apache.avro.specific.SpecificRecordBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class CommonModule extends AbstractModule {

    private static final Logger logger = LoggerFactory.getLogger(CommonModule.class);

    @Override
    protected void configure() {
        bind(BootstrapActor.class).toProvider(new TypeLiteral<BootstrapActorProvider<? extends SpecificRecordBase>>(){}).asEagerSingleton();
        bind(MyConfig.class).toProvider(MyConfigProvider.class).asEagerSingleton();
        bind(StreamBuilder.class).toProvider(StreamBuilderProvider.class);
        bind(new TypeLiteral<EnvelopeHandlerImpl<? extends SpecificRecordBase>>(){})
                .toProvider(new TypeLiteral<EnvelopeHandlerProvider<? extends SpecificRecordBase>>(){}).asEagerSingleton();
    }
}
