package com.click.example.constants;

/**
 * Enum for the Field indexes
 */
public enum RedisFieldIndex {
    PPID(0),
    GUID(1),
    FIRSTNAME(2),
    MIDDLENAME(3),
    LASTNAME(4),
    DOB(5),
    POSTAL_CODE(6),
    GENDER(7);


    private final Integer value;

    RedisFieldIndex(final Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
