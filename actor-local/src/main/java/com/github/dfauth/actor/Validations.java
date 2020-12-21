package com.github.dfauth.actor;

import java.util.UUID;
import java.util.regex.Pattern;

public class Validations {

    private static final String PATTERN = "(/(a-z*A-Z*0-9*-_*))*";
    private static Pattern ID_PATTERN = Pattern.compile(PATTERN);

    public static void validateId(String id) {
        if(ID_PATTERN.matcher(id).matches()) {
            throw  new IllegalArgumentException("Invalid actor name: "+id+" pattern: "+PATTERN);
        }
    }

    public static String anonymousId() {
        return createId(UUID.randomUUID().toString());
    }

    public static String createId(String id) {
        return "/"+id;
    }
}
