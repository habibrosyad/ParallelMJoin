package parallelmjoin;

class Tuple {
    private final long timestamp;
    private final Stream source;
    private final int key;
    private final int value;

    Tuple(long timestamp, Stream source, int key, int value) {
        this.timestamp = timestamp;
        this.source = source;
        this.key = key;
        this.value = value;
    }

    long getTimestamp() {
        return timestamp;
    }

    Stream getSource() {
        return source;
    }

    int getKey() {
        return key;
    }

    int getValue() {
        return value;
    }
}
