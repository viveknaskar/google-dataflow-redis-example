package com.click.example.constants;

/**
 * Enum to store the key prefixes in the Redis instance
 */
public enum KeyPrefix {
    FIRSTNAME("hash1"),
    MIDDLENAME("hash2"),
    LASTNAME("hash3"),
    DOB("hash4"),
    POSTAL_CODE("hash5"),
    GENDER("hash6"),
    PHONE_NUMBER("hash7"),
    GUID("hash11"),
    PPID("hash12");

    private final String value;

    KeyPrefix(final String value) {
        this.value = value;
    }

    @Override
    public String toString() {
        return value;
    }
}
