package parallelmjoin;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class Stats {
    public static final AtomicLong comparison = new AtomicLong();
    public static final AtomicLong output = new AtomicLong();

    public static void run(AtomicInteger barrier, long wait) {
        new Thread(() -> {
            barrier.decrementAndGet();
            while (barrier.get() != 0) ;

            try {
                Thread.sleep(wait);

                // Print report
                System.out.println("ELAPSED=" + wait + "ms");
                System.out.println("OUTPUT=" + output.get());
                System.out.println("COMPARISON=" + comparison.get());
                System.out.println();
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }).start();
    }
}
