package parallelmjoin;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Experiment {
    private static int rate = 1000;
    private static long windowSize = 20000;
    private static String path = "/Users/habib.rosyad/sandbox/MScaleJoin/dataset/shj/1000000/";
    private static Tuple poison;
    private static AtomicInteger barrier;
    private static BlockingQueue<Tuple> queue;

    public static void main(String[] args) throws InterruptedException {
        if (args.length > 2) {
            rate = Integer.parseInt(args[0]);
            windowSize = Long.parseLong(args[1]);
            path = args[2];
        }

        Stream[] sources = Stream.values();

        int numberOfThreads = sources.length;
        poison = new Tuple(-1, null, -1, -1);
        queue = new LinkedBlockingQueue<>();

        // Producers + Consumer + Merger
        ExecutorService pool = Executors.newFixedThreadPool(2 * numberOfThreads + 1);

        // Producers + Consumer + Merger + Stats
        barrier = new AtomicInteger(2 * numberOfThreads + 2);

        Producer[] producers = new Producer[numberOfThreads];
        PThread[] consumers = new PThread[numberOfThreads];
        Merger merger = new Merger(barrier, numberOfThreads);

        // Run stats
        Stats.run(barrier, numberOfThreads, windowSize, rate);

        // Run merger
        pool.submit(merger);

        // Initialise producer and consumer
        for (int i = 0; i < numberOfThreads; i++) {
            producers[i] = new Producer(sources[i]);
            consumers[i] = new PThread(i, sources[i], windowSize, merger, barrier);
            pool.submit(producers[i]);
            pool.submit(consumers[i]);
        }

        // PThread queue feeder
        int poisonCounter = 0;
        while (poisonCounter < numberOfThreads) {
            Tuple newTuple = queue.take();

            if (newTuple == poison) {
                poisonCounter++;
            } else {
                for (int i = 0; i < numberOfThreads; i++) {
                    consumers[i].addToQueue(newTuple);
                }
            }
        }

        // Add poison to all consumers to stop the process
        for (int i = 0; i < numberOfThreads; i++) {
            consumers[i].addToQueue(poison);
        }

        // Wait until Stats marked as finished
        while (!Stats.isDone()) ;

        // Shutdown pool
        pool.shutdownNow();
        pool.awaitTermination(1, TimeUnit.MILLISECONDS);
    }

    private static class Producer implements Runnable {
        private final Stream source;

        Producer(Stream source) {
            this.source = source;
        }

        @Override
        public void run() {
            // Do start until all threads are running
            barrier.getAndDecrement();
            while (barrier.get() != 0) ;

            // For rate control
            long before = 0;
            float ahead = 0;

            // Read local file of the stream
            String filename = path + source;
            try {
                Scanner scanner = new Scanner(new File(filename));

                while (scanner.hasNextLine() && !Stats.isDone()) {
                    String[] keyval = scanner.nextLine().trim().split("\\s+");
                    queue.put(new Tuple(System.currentTimeMillis(), source,
                            Integer.parseInt(keyval[0]), Integer.parseInt(keyval[1])));

                    // Rate control
                    long now = System.nanoTime() / 1000000L;
                    if (before != 0) {
                        ahead -= (float) (now - before) / 1000 * rate - 1;
                        if (ahead > 0) {
                            Thread.sleep((long) (ahead / rate * 1000));
                        }
                    }
                    before = now;
                }

                queue.put(poison);
            } catch (FileNotFoundException e) {
                System.out.println(filename + " not found");
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }
        }
    }
}
