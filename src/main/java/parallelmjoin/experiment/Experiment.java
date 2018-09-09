package parallelmjoin.experiment;

import parallelmjoin.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class Experiment {
    private static Tuple poison;
    private static AtomicInteger barrier;
    private static BlockingQueue<Tuple> queue;

    public static void main(String[] args) throws InterruptedException {
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
        Stats.run(barrier, 20000);

        // Run merger
        pool.submit(merger);

        // Initialise producer and consumer
        for (int i = 0; i < numberOfThreads; i++) {
            producers[i] = new Producer(sources[i]);
            consumers[i] = new PThread(i, sources[i], 4000000, merger, barrier);
            pool.submit(producers[i]);
            pool.submit(consumers[i]);
        }

//        long start = System.nanoTime();

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
        while (!Stats.finished.get()) ;

        pool.shutdownNow();
        pool.awaitTermination(1, TimeUnit.MILLISECONDS);

//        while (Stats.output.get() != 300000) ;
//
//        System.out.println("Finish in " + (System.nanoTime() - start) / 1000000 + "ms, output " + Stats.output.get());
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

//            long start = System.nanoTime();
            // Read local file of the stream
//            String filename = "/Users/habib.rosyad/sandbox/MScaleJoin/dataset/shj/1000000/" + source;
            String filename = source.toString();
            int timestamp = 0;
            try {
                Scanner scanner = new Scanner(new File(filename));

                while (scanner.hasNextLine() && !Stats.finished.get()) {
                    String[] keyval = scanner.nextLine().trim().split("\\s+");
                    queue.put(new Tuple(timestamp++, source,
                            Integer.parseInt(keyval[0]), Integer.parseInt(keyval[1])));
                }

                queue.put(poison);
            } catch (FileNotFoundException e) {
                System.out.println(filename + " not found");
            } catch (InterruptedException e) {
                System.out.println(e.getMessage());
            }

//            System.out.println("Finish with " + source + " total " + timestamp + " in " + (System.nanoTime() - start) / 1000000 + "ms");
        }
    }
}
