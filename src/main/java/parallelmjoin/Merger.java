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
    private int output;

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

        while (true) {
            int matches = 1;
            for (int i = 0; i < numberOfThreads; i++) {
                try {
                    matches *= bucket.get(i).take().size();
                } catch (InterruptedException e) {
                    System.out.println(e.getMessage());
                }
            }

            output += matches;
            Stats.output.addAndGet(matches);
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

    public int getOutput() {
        return output;
    }
}
