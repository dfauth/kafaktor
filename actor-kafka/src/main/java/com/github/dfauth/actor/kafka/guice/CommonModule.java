package com.github.dfauth.actor.kafka.guice;

import com.github.dfauth.actor.kafka.EnvelopeHandler;
import com.github.dfauth.actor.kafka.bootstrap.BootstrapActor;
import com.github.dfauth.actor.kafka.guice.providers.BootstrapActorProvider;
import com.github.dfauth.actor.kafka.guice.providers.EnvelopeHandlerProvider;
import com.github.dfauth.actor.kafka.guice.providers.MyConfigProvider;
import com.github.dfauth.actor.kafka.guice.providers.StreamBuilderProvider;
import com.github.dfauth.kafka.Stream;
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
        bind(new TypeLiteral<Stream.Builder<String, byte[]>>(){}).toProvider(StreamBuilderProvider.class).asEagerSingleton();
        bind(new TypeLiteral<EnvelopeHandler<SpecificRecordBase>>(){})
                .toProvider(new TypeLiteral<EnvelopeHandlerProvider>(){}).asEagerSingleton();
    }
}
