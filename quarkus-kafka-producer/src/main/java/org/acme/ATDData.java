package org.acme;

public class ATDData {
    String key;
    String value;

    public ATDData(String key, String value) {
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