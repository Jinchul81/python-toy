import com.skt.dbp.wsdl.Client;
import com.skt.dbp.utility.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by jinchulkim on 2017. 7. 13..
 */
public class ParallelLoadTest extends AbstractLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(ParallelLoadTest.class);

    class Chunk extends Vector<String> {
    }

    class SharedQueues {
        public SharedQueues() {
            readyQueue = new ConcurrentLinkedDeque<Chunk>();
            todoQueue = new ConcurrentLinkedDeque<Chunk>();

            for (int i = 0; i < numOfChunks; ++i) {
                readyQueue.push(new Chunk());
            }
        }

        public String toString() {
            return "SharedQueues = {readyQueue = "
                    + readyQueue.size() + ", todoQueue = " + todoQueue.size() + "}";
        }

        public ConcurrentLinkedDeque<Chunk> readyQueue;
        public ConcurrentLinkedDeque<Chunk> todoQueue;

        final int numOfChunks = 100;
    }

    class LoaderThread extends Thread {
        //private int numCountForRequest = 100;
        //private int chunkSize = 10;
        private int numCountForRequest = 10;
        private int chunkSize = 10;
        private Context context;

        public LoaderThread(final Context context) {
            this.context = context;
        }

        @Override
        public void run() {
            logger.error("* LoaderThread start");
            //final String fileName = new String(System.getProperty("user.dir") + "/data/addr.txt");
            final String fileName = new String(System.getProperty("user.dir") + "/data/addr.txt.30k");
            FileInputStream fileInputStream = null;
            BufferedReader bufferedReader = null;
            SharedQueues sharedQueues = context.getSharedQueues();
            try {
                fileInputStream = new FileInputStream(fileName);
                bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

                Chunk chunk = null;
                while (! context.isStopped()) {
                    while ((chunk = sharedQueues.readyQueue.poll()) == null) {
                        Thread.sleep(TEN_MILLI_SECS);
                    }

                    for (int i = 0; i < chunkSize; ++i) {
                        String element = new String();
                        int j = 0;
                        for (; j < numCountForRequest; ++j) {
                            String line;
                            if ((line = bufferedReader.readLine()) != null) {
                                if (element.isEmpty()) {
                                    element += context.getAuthKey();
                                }
                                element += "\n" + (j+1) + "|" + line;
                            } else {
                               context.isStopped(true);
                               break;
                            }
                        }

                        if (! element.isEmpty()) {
                            chunk.add(element);
                        } else {
                            break;
                        }
                    }

                    sharedQueues.todoQueue.push(chunk);
                }
                logger.error("[LoaderThread]" + sharedQueues.toString());
            } catch (IOException e) {
                logger.error("Unexpected error {}", e);
            } catch (InterruptedException e) {
                logger.error("Unexpected error {}", e);
            } finally {
                if (null != bufferedReader) {
                    try {
                        bufferedReader.close();
                    } catch (IOException e) { logger.error("Uexpected error {}", e); }
                }
                if (null != fileInputStream) {
                    try {
                        fileInputStream.close();
                    } catch (IOException e) { logger.error("Uexpected error {}", e); }
                }
            }
        }
    }

    class WorkerThread extends Thread {
        private Context context;
        private long accumulatedThroughput;
        private long elapsedTime;
        private String prefix;
        private String result;

        public WorkerThread(final Context context) {
            this.context = context;
            this.prefix = "[WorkerThread-" + getId() + "]";
            this.result = "";
        }

        public boolean isStopped() {
            return (context.isStopped() && context.getSharedQueues().todoQueue.isEmpty());
        }

        private String getPrefix() {
            return prefix;
        }

        @Override
        public void run() {
            try {
                execute();
            } catch (Exception e) {
                logger.error("Unexpected error {]", e);
            }
        }

        private void execute() throws Exception {
            logger.error("{} start", getPrefix());
            final SharedQueues sharedQueues = context.getSharedQueues();
            Chunk chunk = null;
            final Worker worker = new Worker(context.getClient());
            while (! isStopped()) {
                while ((chunk = sharedQueues.todoQueue.poll()) == null) {
                    logger.error("{} {}", getPrefix(), sharedQueues.toString());
                    try {
                        Thread.sleep(TEN_MILLI_SECS);
                    } catch (InterruptedException e) {
                        logger.error("Unexpected error {}", e);
                    }
                    if (isStopped()) break;

                }

                if (null == chunk) {
                    continue;
                }

                for (final String input : chunk) {
                    worker.run(input);
                }
                saveResult(chunk, worker);

//                logger.error("{}", chunk.size());
//                logger.error("{}", chunk.toString());

                chunk.clear();

                sharedQueues.readyQueue.push(chunk);
            }
        }

        public long getAccumulatedThroughput() {
            return accumulatedThroughput;
        }

        public long getElapsedTime() {
            return elapsedTime;
        }

        private void saveResult(final Chunk chunk, final Worker worker) {
            accumulatedThroughput += chunk.size();
            elapsedTime += worker.getElapsedTime();
            result += worker.getResult();

//            logger.error("{} numCount: {}, elapsedTime: {}, result: \n{}", getPrefix(), chunk.size(), worker.getElapsedTime(), result);
        }

        public String toString() {
            return prefix + " accumulatedThroughput: " + getAccumulatedThroughput() + ", elapsedTime: " + getElapsedTime();
        }
    }

    public class Context {
        public Context(final Client client
                     , final String authKey
                     , final SharedQueues sharedQueues) {
            this.client = client;
            this.authKey = authKey;
            this.sharedQueues = sharedQueues;
        }

        public final Client getClient() {
            return client;
        }
        public final String getAuthKey() { return authKey; }
        public final SharedQueues getSharedQueues() { return sharedQueues; }
        public boolean isStopped() {
            return stopped;
        }

        public void isStopped(boolean b) {
            stopped = b;
        }

        private final Client client;
        private final String authKey;
        private final SharedQueues sharedQueues;

        private boolean stopped;
    }

    @Override
    public void run() {
        final SharedQueues sharedQueues = new SharedQueues();
        final Context context = new Context(super.client, super.authKey, sharedQueues);
        //final int [] numThreads = new int[]{1, 2, 4, 8, 16, 32, 64, 128};
        //final int [] numThreads = new int[]{16, 32, 64, 128, 256, 512};
        //final int [] numThreads = new int[]{1, 2};
        //final int [] numThreads = new int[]{2, 4};
        //final int [] numThreads = new int[]{4, 8, 16, 32};
        //final int [] numThreads = new int[]{4, 8};
        final int [] numThreads = new int[]{32};

        for (int i = 0; i < numThreads.length; ++i) {
            context.isStopped(false);
            final int numThread = numThreads[i];
            final ArrayList<Thread> threads = new ArrayList<Thread>();
            final ArrayList<WorkerThread> workerThreads= new ArrayList<WorkerThread>();
            final LoaderThread loaderThread = new LoaderThread(context);
            threads.add(loaderThread);
            loaderThread.start();

            for (int j = 0; j < numThread; ++j) {
                WorkerThread workerThread = new WorkerThread(context);
                threads.add(workerThread);
                workerThreads.add(workerThread);
            }

            for (WorkerThread workerThread : workerThreads) {
                workerThread.start();
            }

            try {
                for (final Thread thread : threads) {
                    thread.join();
                }
            } catch (InterruptedException ie) {
                logger.error("Unexpected error: {}",  ie);
            }

            long accumulatedThroughput = 0;
            long accumulatedElapsedTime = 0;

            for (final WorkerThread workerThread : workerThreads) {
                if (workerThread.getElapsedTime() != 0) {
                    accumulatedThroughput += workerThread.getAccumulatedThroughput();
                    accumulatedElapsedTime += workerThread.getElapsedTime();
                    logger.error(workerThread.toString());
                }
            }

            final double elapsedTime = accumulatedElapsedTime / numThread;
            logger.error("# of threads: {}, TPS: {}, Accumulated throughput: {}, Elapsed time: {}"
                    , numThread, accumulatedThroughput / (elapsedTime / 1000), accumulatedThroughput, elapsedTime);
        }
    }

    public static void main(String [] args) {
        final ParallelLoadTest test = new ParallelLoadTest();
        test.run();
    }
}
