package parallelmjoin;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Collecting stats for measuring performance during an experiment.
 */
class Stats {
    private static final String separator = ",";
    private static final AtomicLong latency = new AtomicLong(); // Sum of latency
    private static final AtomicLong latencyCounter = new AtomicLong();
    private static final AtomicLong processed = new AtomicLong(); // Tuple processed
    private static final AtomicLong comparison = new AtomicLong();
    private static final AtomicLong output = new AtomicLong();
    private static final AtomicBoolean enabled = new AtomicBoolean();
    private static final AtomicBoolean done = new AtomicBoolean();

    static void run(AtomicInteger barrier, int numberOfThreads, long windowSize, int rate) {
        new Thread(() -> {
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            try {
                // Warming up, up to the length of the window
                Thread.sleep(windowSize);

                // Measure for 10 times
                for (int i = 0; i < 5; i++) {
                    enabled.set(true);

                    // Measure stats for 10s
                    Thread.sleep(10000);
                    enabled.set(false);

                    // Produce CSV like data to ease the analysis
                    // [trial_id, threads, window_ms, rate_s,
                    // latency_ms, processed_s, output_s, comparison_s, comparison_avg_s]
                    System.out.println(i + separator +
                            numberOfThreads + separator +
                            windowSize + separator +
                            rate + separator +
                            latency.get() / (latencyCounter.get() > 0 ? latencyCounter.get() : 1 )  + separator +
                            processed.get() / 10 + separator +
                            output.get() / 10 + separator +
                            comparison.get() / 10 + separator +
                            comparison.get() / 10 / numberOfThreads
                    );

                    // Add delay of 1s between trials
                    Thread.sleep(1000);

                    // Reset stats
                    latency.set(0);
                    processed.set(0);
                    comparison.set(0);
                    output.set(0);
                }

                // Set to finish, kill all threads
                done.set(true);
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }).start();
    }

    static void addLatency(long delta) {
        if (enabled.get()) {
            latency.addAndGet(delta);
            latencyCounter.incrementAndGet();
        }
    }

    static void incrementProcessed() {
        if (enabled.get()) {
            processed.incrementAndGet();
        }
    }

    static void incrementComparison() {
        if (enabled.get()) {
            comparison.incrementAndGet();
        }
    }

    static void addOutput(long delta) {
        if (enabled.get()) {
            output.addAndGet(delta);
        }
    }

    static boolean isDone() {
        return done.get();
    }
}
