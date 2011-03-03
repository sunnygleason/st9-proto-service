package com.g414.st9.proto.service.store;

/**
 * Putting the "Key" in Key-Value storage.
 */
public class Key {
    private final String identifier;
    private final String type;
    private final Long id;

    public Key(String type, Long id) {
        this.identifier = type + ":" + Long.toString(id);
        this.type = type;
        this.id = id;
    }

    public static Key valueOf(String key) {
        String[] parts = key.split(":");
        if (parts.length != 2) {
            throw new IllegalArgumentException("Invalid key");
        }

        Long id = Long.parseLong(parts[1]);

        return new Key(parts[0], id);
    }

    public Long getId() {
        return id;
    }

    public String getType() {
        return type;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Key && identifier.equals(((Key) obj).identifier);
    }

    @Override
    public int hashCode() {
        return identifier.hashCode();
    }

    @Override
    public String toString() {
        return identifier;
    }
}
