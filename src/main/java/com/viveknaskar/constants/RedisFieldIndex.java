package com.viveknaskar.constants;

/**
 * Enum for the Field indexes
 */
public enum RedisFieldIndex {
    GUID(0),
    PPID(1),
    FIRSTNAME(2),
    MIDDLENAME(3),
    LASTNAME(4),
    DOB(5),
    POSTAL_CODE(6),
    GENDER(7),
    PHONE_NUMBER(8);


    private final Integer value;

    RedisFieldIndex(final Integer value) {
        this.value = value;
    }

    public Integer getValue() {
        return value;
    }
}
