package parallelmjoin;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;

public class PThread implements Runnable {
    private final int id;
    private final Window window;
    private final Stream source;
    private final Merger merger;
    private final AtomicInteger barrier;
    private final BlockingQueue<Tuple> queue;

    public PThread(int id, Stream source, long windowSize, Merger merger, AtomicInteger barrier) {
        this.id = id;
        this.source = source;
        this.window = new WindowImpl(windowSize);
        this.merger = merger;
        this.barrier = barrier;
        this.queue = new LinkedBlockingQueue<>();
    }

    @Override
    public void run() {
        // Do start until all threads are running
        barrier.getAndDecrement();
        while (barrier.get() != 0) ;

        // barrier will be used again to sync the pipeline between PThread and Merger
        while (!Stats.finished.get()) {
            try {
                // Block until there is a tuple to be processed
                Tuple tuple = queue.take();

                // If the tuple is poison
                if (tuple.getTimestamp() == -1) break;

                // If the tuple comes from the same source do not probe
                if (tuple.getSource() != source) {
                    // Expire the window before probing
                    window.expire(tuple.getTimestamp());

                    // Increase stats
                    Stats.comparison.getAndIncrement();

                    // Probe the window
                    List<Tuple> matches = new ArrayList<>();
                    matches.addAll(window.probe(tuple));

                    // Creating a new list is a must, since Multimap sometimes will return list with same reference
                    merger.addAll(id, matches);
                } else {
                    // Add tuple to the window
                    window.insert(tuple);

                    // Add to merger
                    merger.add(id, tuple);
                }
            } catch (InterruptedException e) {
                System.out.print(e.getMessage());
            }
        }
    }

    public void addToQueue(Tuple tuple) {
        queue.add(tuple);
    }
}
