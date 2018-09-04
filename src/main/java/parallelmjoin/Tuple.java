package parallelmjoin;

public class Tuple {
    private final long timestamp;
    private final Stream source;
    private final int key;
    private final int value;

    public Tuple(long timestamp, Stream source, int key, int value) {
        this.timestamp = timestamp;
        this.source = source;
        this.key = key;
        this.value = value;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public Stream getSource() {
        return source;
    }

    public int getKey() {
        return key;
    }

    public int getValue() {
        return value;
    }
}
