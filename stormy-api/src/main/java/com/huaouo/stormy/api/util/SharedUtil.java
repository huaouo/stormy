package com.huaouo.stormy.api.util;

public class SharedUtil {
    public static void validateId(String id) {
        if (!id.matches("[a-zA-Z0-9]+")) {
            throw new IllegalArgumentException("Only alphanumeric characters allowed for id");
        }
    }
}
