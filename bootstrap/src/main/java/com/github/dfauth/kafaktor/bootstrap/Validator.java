package com.github.dfauth.kafaktor.bootstrap;

import java.util.*;
import java.util.function.BinaryOperator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public interface Validator<T> {

    static <T> Predicate<T> validatePredicates(Predicate<T>... predicates) {
        BinaryOperator<Optional<T>> acc = (o1,o2) -> o1.flatMap(_o -> o2.map(_o2 -> Optional.ofNullable(_o2))).orElse(Optional.empty());
        Stream<Predicate<T>> s = Stream.of(predicates);
        return t -> {
            Optional<T> o = Optional.ofNullable(t);
            return s.map(p -> o.filter(p)).reduce(o, acc).isPresent();
        };
    }

    static <T> Function<T,List<Validity<T>>> validate(Validator<T>... validators) {
        return t -> Stream.of(validators).map(v -> v.validate(t)).collect(Collectors.toList());
    }

    Validity<T> validate(T t);

    interface Validity<T> {

        static <T> Valid<T> valid(T t) {
            return new Valid(t);
        }

        static <T> Invalid<T> invalid(Throwable t) {
            return new Invalid(t);
        }

        default boolean isValid() {
            return true;
        }

        default boolean isInvalid() {
            return !isValid();
        }

        Validity<T> fold(Validity<T> v);
    }

    class Valid<T> implements Validity<T> {

        private final T t;

        public Valid(T t) {
            this.t = t;
        }

        @Override
        public Validity<T> fold(Validity<T> v) {
            return v.isValid() ? this : v;
        }
    }

    class Invalid<T> implements Validity<T> {

        private final List<Throwable> validationExceptions;

        public Invalid(Throwable... validationExceptions) {
            this(Arrays.asList(validationExceptions));
        }

        public Invalid(List<Throwable> validationExceptions) {
            this.validationExceptions = validationExceptions;
        }

        @Override
        public boolean isValid() {
            return false;
        }

        @Override
        public Validity<T> fold(Validity<T> v) {
            return v.isValid() ? this : combine((Invalid<T>) v);
        }

        private Invalid<T> combine(Invalid<T> v) {
            List<Throwable> tmp = new ArrayList<>(validationExceptions);
            tmp.addAll(v.validationExceptions);
            return new Invalid<>(tmp);
        }
    }
}
