package parallelmjoin;

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

    public Merger(AtomicInteger barrier, int numberOfThreads) {
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
            while (!Stats.finished.get()) {
                int matches = 1;
                for (int i = 0; i < numberOfThreads; i++) {
                    matches *= bucket.get(i).take().size();
                }

                Stats.output.addAndGet(matches);

                if (matches > 0) Stats.initialResponse.compareAndSet(0, System.nanoTime());
            }
        } catch (InterruptedException e) {
//            System.out.println(e.getMessage());
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
