package parallelmjoin;

import java.util.List;

public interface Window {
    void insert(Tuple tuple);

    List<Tuple> probe(Tuple tuple);

    void expire(long timestamp);

    int size();
}