package parallelmjoin;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Stats {
    static final AtomicLong initialResponse = new AtomicLong(); // In nanos
    static final AtomicLong comparison = new AtomicLong();
    static final AtomicLong output = new AtomicLong();
    public static final AtomicBoolean finished = new AtomicBoolean();

    public static void run(AtomicInteger barrier, long wait) {
        new Thread(() -> {
            long start, elapsed;

            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            start = System.nanoTime();

            try {
                Thread.sleep(wait);

                // Set to finish, kill all threads
                finished.set(true);
                elapsed = (System.nanoTime() - start) / 1000000000; // In seconds

                // Print report
//                System.out.println("ELAPSED=" + elapsed + "s");
//                System.out.println("INITIAL_RESPONSE=" + (initialResponse.get() - start) / 1000000 + "ms");
//                System.out.println("OUTPUT_TOTAL=" + output.get());
//                System.out.println("OUTPUT/s=" + output.get() / elapsed);
//                System.out.println("COMPARISON_TOTAL=" + comparison.get());
//                System.out.println("COMPARISON/s=" + comparison.get() / elapsed);
//                System.out.println();

                // Produce CSV like data to ease the analysis
                // [elapsed_s, initial_response_ms, output_total, output_s, comparison_total, comparison_s]
                System.out.println(elapsed + "," +
                        (initialResponse.get() - start) / 1000000 + "," +
                        output.get() + "," +
                        output.get() / elapsed + "," +
                        comparison.get() + "," +
                        comparison.get() / elapsed
                );
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }).start();
    }
}
