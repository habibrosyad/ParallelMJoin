package parallelmjoin.experiment;

import parallelmjoin.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Experiment {
    static long WINDOW_SIZE;
    static int NUMBER_OF_THREADS;
    static int EXPECTED_OUTPUT;
    static long STATS_WAIT;
    static Tuple POISON;
    static ExecutorService pool;
    static AtomicInteger barrier;
    static BlockingQueue<Tuple> queue;

    public static void main(String[] args) throws InterruptedException {
        Stream[] sources = Stream.values();

        WINDOW_SIZE = 4000000;
        EXPECTED_OUTPUT = 300000;
        NUMBER_OF_THREADS = sources.length;
        STATS_WAIT = 20000;
        POISON = new Tuple(-1, null, -1, -1);
        queue = new LinkedBlockingQueue<>();

        // Producers + Consumer + Merger
        pool = Executors.newFixedThreadPool(2 * NUMBER_OF_THREADS + 1);

        // Producers + Consumer + Merger + Stats
        barrier = new AtomicInteger(2 * NUMBER_OF_THREADS + 2);

        Producer[] producers = new Producer[NUMBER_OF_THREADS];
        PThread[] consumers = new PThread[NUMBER_OF_THREADS];
        Merger merger = new Merger(barrier, NUMBER_OF_THREADS);

        // Run stats
        Stats.run(barrier, STATS_WAIT);

        // Run merger
        pool.submit(merger);

        // Initialise producer and consumer
        for (int i = 0; i < NUMBER_OF_THREADS; i++) {
            producers[i] = new Producer(sources[i]);
            consumers[i] = new PThread(i, sources[i], WINDOW_SIZE, merger, barrier);
            pool.submit(producers[i]);
            pool.submit(consumers[i]);
        }

        long start = System.nanoTime();

        // PThread queue feeder
        int poisonCounter = 0;
        while (poisonCounter < NUMBER_OF_THREADS) {
            Tuple newTuple = queue.take();

            if (newTuple == POISON) {
                poisonCounter++;
            } else {
                for (int j = 0; j < NUMBER_OF_THREADS; j++) {
                    consumers[j].addToQueue(newTuple);
                }
            }
        }

        while (Stats.output.get() != EXPECTED_OUTPUT) ;

        System.out.println("Finish in " + (System.nanoTime() - start) / 1000000 + "ms, output " + Stats.output.get());
    }

    private static class Producer implements Runnable {
        private final Stream source;

        public Producer(Stream source) {
            this.source = source;
        }

        @Override
        public void run() {
            // Do start until all threads are running
            barrier.getAndDecrement();
            while (barrier.get() != 0) ;

            long start = System.nanoTime();
            //Read local file of the stream
            //String filename = "/Users/habib.rosyad/sandbox/MScaleJoin/dataset/shj/1000000/" + source;
            String filename = source.toString();
            int timestamp = 0;
            try {
                Scanner scanner = new Scanner(new File(filename));

                while (scanner.hasNextLine()) {
                    String[] keyval = scanner.nextLine().trim().split("\\s+");
                    queue.put(new Tuple(timestamp++, source,
                            Integer.parseInt(keyval[0]), Integer.parseInt(keyval[1])));
                }

                queue.put(POISON);
            } catch (FileNotFoundException e) {
                System.out.println(filename + " not found");
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

            System.out.println("Finish with " + source + " total " + timestamp + " in " + (System.nanoTime() - start) / 1000000 + "ms");
        }
    }
}
