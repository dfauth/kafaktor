package com.github.dfauth.actor.kafka.guice;

import com.google.inject.Module;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

public class MyModules {

    private static List<Module> registered = new ArrayList<>();

    public static Iterable<Module> get() {
        return registered;
    }

    public static void register(Module... modules) {
        Stream.of(modules).forEach(m -> registered.add(m));
    }

    public static Iterable<? extends Module> getOrElse(Module... modules) {
        return registered.size() > 0 ? registered : Arrays.asList(modules);
    }
}
