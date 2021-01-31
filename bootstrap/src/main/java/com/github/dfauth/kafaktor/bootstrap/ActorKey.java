package com.github.dfauth.kafaktor.bootstrap;

import com.github.dfauth.partial.Tuple2;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.dfauth.Lists.append;
import static com.github.dfauth.Lists.segment;

public interface ActorKey {

    String head();

    Optional<ActorKey> tail();

    int size();

    ActorKey add(String name);

    static ActorKey toActorKey(List<String> l) {
        Objects.requireNonNull(l);
        if(l.size() == 0) {
            throw new IllegalArgumentException("empty actor key is not supported");
        }
        Tuple2<String, List<String>> t = segment(l);
        return new ActorKey() {
            @Override
            public String head() {
                return t._1();
            }

            @Override
            public Optional<ActorKey> tail() {
                return Optional.ofNullable(t._2()).filter(l -> !l.isEmpty()).map(l -> toActorKey(l));
            }

            @Override
            public int size() {
                return t._2().size()+1;
            }

            @Override
            public ActorKey add(String name) {
                return toActorKey(append(l, name));
            }

            @Override
            public String toString() {
                return l.stream().collect(Collectors.joining("/","/",""));
            }
        };
    }

    static ActorKey toActorKey(String key) {
        return toActorKey(Stream.of(key.split("/")).filter(s -> !s.isEmpty()).collect(Collectors.toList()));
    }

    static Predicate<ActorKey> headIs(String name) {
        return a -> a.head().equals(name);
    }

    Predicate<ActorKey> noTail = a -> a.tail().isEmpty();

}
