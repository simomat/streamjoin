package de.infonautika.streamjoin;

class Guards {
    static void checkNotNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }
}
