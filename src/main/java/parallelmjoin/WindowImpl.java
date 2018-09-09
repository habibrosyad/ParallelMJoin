package parallelmjoin;

import com.google.common.collect.LinkedListMultimap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class WindowImpl implements Window {
    private final LinkedListMultimap<Integer, Tuple> internal;
    private final long size;

    WindowImpl(long size) {
        this.size = size;
        internal = LinkedListMultimap.create();
    }

    @Override
    public void insert(Tuple tuple) {
        internal.put(tuple.getKey(), tuple);
    }

    @Override
    public List<Tuple> probe(Tuple tuple) {
        return internal.get(tuple.getKey());
    }

    @Override
    public void expire(long timestamp) {
        Iterator<Map.Entry<Integer, Tuple>> iter = internal.entries().iterator();

        while (iter.hasNext()) {
            if (Math.abs(iter.next().getValue().getTimestamp() - timestamp) > size) {
                iter.remove();
            } else {
                break;
            }
        }
    }
}
