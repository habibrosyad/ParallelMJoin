package parallelmjoin;

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class Merger implements Runnable {
    private final AtomicInteger barrier;
    private final List<BlockingQueue<List<Tuple>>> bucket;
    private final int numberOfThreads;

    Merger(AtomicInteger barrier, int numberOfThreads) {
        this.barrier = barrier;
        this.numberOfThreads = numberOfThreads;
        bucket = new ArrayList<>();

        // Initialise intermediate results bucket
        for (int i = 0; i < numberOfThreads; i++) {
            bucket.add(new LinkedBlockingQueue<>());
        }
    }

    @Override
    public void run() {
        // Do start until all threads are running
        barrier.getAndDecrement();
        while (barrier.get() != 0) ;

        try {
            while (!Stats.isDone()) {
                List<List<Tuple>> product = new ArrayList<>();
                long maxTimestamp = 0;

                for (int i = 0; i < numberOfThreads; i++) {
                    List<Tuple> matches = bucket.get(i).take();
                    product.add(matches);
                    // For latency calculation
                    for (Tuple match : matches) {
                        if (match.getTimestamp() > maxTimestamp) {
                            maxTimestamp = match.getTimestamp();
                        }
                    }
                }

                List<List<Tuple>> output = Lists.cartesianProduct(product);

                if (output.size() > 0) {
                    Stats.addLatency(System.currentTimeMillis() - maxTimestamp);
                }

                Stats.addOutput(output.size());
            }
        } catch (InterruptedException e) {
        }
    }

    void addAll(int id, List<Tuple> tuples) {
        try {
            bucket.get(id).put(tuples);
        } catch (InterruptedException e) {
            System.out.println(e.getMessage());
        }
    }

    void add(int id, Tuple tuple) {
        addAll(id, Collections.singletonList(tuple));
    }
}
