package org.acme;

public class RNData {
    String key;
    String value;

    public RNData(String key, String value) {
        this.key = key;
        this.value = value;
    }

    String getKey() {
        return key;
    }

    String getValue() {
        return value;
    }
}