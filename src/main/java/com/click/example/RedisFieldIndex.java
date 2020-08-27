package com.click.example;

/**
 * Enum for the Field indexes
 */
public enum RedisFieldIndex {
    FIRSTNAME(1),
    MIDDLENAME(2),
    LASTNAME(3),
    DOB(4),
    POSTAL_CODE(5),
    GENDER(6),
    GUID(7),
    PPID(8);

    private final Integer value;

    RedisFieldIndex(final Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
