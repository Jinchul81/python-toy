package com.skt.dbp.launcher;

import com.skt.dbp.config.ClientAppConfig;
import com.skt.dbp.config.DevClientAppConfig;
import com.skt.dbp.config.ProductClientAppConfig;
import com.skt.dbp.utility.StringHelper;
import com.skt.dbp.wsdl.Client;
import com.skt.dbp.utility.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.util.Assert;
import org.springframework.ws.client.WebServiceIOException;

import java.io.*;
import java.net.ConnectException;
import java.util.ArrayList;
import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedDeque;

/**
 * Created by jinchulkim on 2017. 7. 13..
 */
public class WSDLClient {
    private static final Logger logger = LoggerFactory.getLogger(WSDLClient.class);
    private static int NUM_RECORDS_IN_REQUEST = 50;
    private static int NUM_REQUESTS_IN_CHUNK = 10;
    private static int NUM_CHUNKS_IN_QUEUE = 1000;
    private static char SEPARATOR_FOR_INPUT = 0x1;
    private static char BAR_SEPARATOR = '|';

    private final AnnotationConfigApplicationContext ctx = new AnnotationConfigApplicationContext();
    private final DevClientAppConfig devClientAppConfig;
    private final ProductClientAppConfig productClientAppConfig;
    private final Client devClient;
    private final Client productClient;
    private final int TEN_MILLI_SECS = 10;
    private final String inputFilename;
    private final String outputFilename;

    public WSDLClient(final String inputFilename, final String outputFilename) {
        devClientAppConfig = new DevClientAppConfig();
        productClientAppConfig = new ProductClientAppConfig();
        devClient = devClientAppConfig.convertClient(devClientAppConfig.marshaller());
        productClient = productClientAppConfig.convertClient(productClientAppConfig.marshaller());
        this.inputFilename = inputFilename;
        this.outputFilename = outputFilename;
    }

    class Chunk extends Vector<String> {
    }

    class SharedQueues {
        public SharedQueues() {
            readyQueue = new ConcurrentLinkedDeque<Chunk>();
            todoQueue = new ConcurrentLinkedDeque<Chunk>();
            doneQueue = new ConcurrentLinkedDeque<Chunk>();

            for (int i = 0; i < numChunksInQueue; ++i) {
                readyQueue.push(new Chunk());
            }
        }

        public String toString() {
            return "SharedQueues = {"
                    + "  readyQueue = " + readyQueue.size()
                    + ", todoQueue = " + todoQueue.size()
                    + ", doneQueue = " + doneQueue.size()
                    + "  }";
        }

        public boolean isDone() {
            return todoQueue.isEmpty() && doneQueue.isEmpty() && readyQueue.size() == numChunksInQueue;
        }

        public ConcurrentLinkedDeque<Chunk> readyQueue;
        public ConcurrentLinkedDeque<Chunk> todoQueue;
        public ConcurrentLinkedDeque<Chunk> doneQueue;

        final int numChunksInQueue = WSDLClient.NUM_CHUNKS_IN_QUEUE;
    }

    class Writer extends Thread {
        private Context context;
        private String prefix;

        public Writer(final Context context) {
            this.context = context;
            this.prefix = "[Writer-" + getId() + "]";
        }

        public boolean isStopped() {
            return context.error() || (context.isStopped() && context.getSharedQueues().isDone());
        }

        private String getPrefix() {
            return prefix;
        }

        @Override
        public void run() {
            try {
                execute();
            } catch(Exception e) {
                logger.error("Unexpected error {}", e);
                context.error(true);
            }
        }

        private void execute() throws Exception {
//            logger.error("* Writer start");
            BufferedWriter bufferedWriter = null;
            SharedQueues sharedQueues = context.getSharedQueues();
            try {
                final boolean append = false;
                bufferedWriter = new BufferedWriter(new FileWriter(new File(context.getOutputFilename()), append));

                Chunk chunk = null;
                long count = 0;
                while (! isStopped()) {
                    while ((chunk = sharedQueues.doneQueue.poll()) == null) {
//                        logger.error("{} {}", getPrefix(), context.toString());
                        Thread.sleep(TEN_MILLI_SECS);
                        if (isStopped()) break;
                    }

                    if (null == chunk) continue;

                    if (! chunk.isEmpty()) {
                        for (final String input : chunk) {
                            count++;
                            bufferedWriter.write(input + "\n");
                        }

                        chunk.clear();
                    }
                    sharedQueues.readyQueue.push(chunk);

                    if (count > 0 && count % 1000 == 0) {
                        logger.info("{} transacted count={}", getPrefix(), count);
                    }
                }
            } catch (IOException e) {
                logger.error("Unexpected error {}", e);
                throw e;
            } finally {
                if (null != bufferedWriter) {
                    try {
                        bufferedWriter.flush();
                    } catch (IOException e) { logger.error("Uexpected error {}", e); }
                    try {
                        bufferedWriter.close();
                    } catch (IOException e) { logger.error("Uexpected error {}", e); }
                }
            }
        }
    }

    class Reader extends Thread {
        //private int numCountForRequest = 100;
        //private int chunkSize = 10;
        private int numRecordsInRequest = WSDLClient.NUM_RECORDS_IN_REQUEST;
        private int numRequestsInChunk = WSDLClient.NUM_REQUESTS_IN_CHUNK;
        private Context context;
        private String prefix;

        public Reader(final Context context) {
            this.context = context;
            this.prefix = "[Reader-" + getId() + "]";
        }

        private String getPrefix() {
            return prefix;
        }

        public boolean isStopped() {
            return context.error() || context.isStopped();
        }

        @Override
        public void run() {
            try {
                execute();
            } catch(Exception e) {
                logger.error("Unexpected error {}", e);
                context.error(true);
            }
        }

        private void execute() throws Exception {
//            logger.error("* Reader start");
            FileInputStream fileInputStream = null;
            BufferedReader bufferedReader = null;
            SharedQueues sharedQueues = context.getSharedQueues();
            try {
                fileInputStream = new FileInputStream(context.getInputFilename());
                bufferedReader = new BufferedReader(new InputStreamReader(fileInputStream));

                Chunk chunk = null;
                while (! isStopped()) {
                    while ((chunk = sharedQueues.readyQueue.poll()) == null) {
//                        logger.error("{} {}", getPrefix(), context.toString());
                        Thread.sleep(TEN_MILLI_SECS);

                        if (isStopped()) break;
                    }
//                    logger.error("{} {}", getPrefix(), context.toString());

                    if (null == chunk) continue;

                    for (int i = 0; i < numRequestsInChunk; ++i) {
                        String element = new String();
                        int j = 0;
                        for (; j < numRecordsInRequest; ++j) {
                            String line;
                            if ((line = bufferedReader.readLine()) != null) {
                                element += line + "\n";
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
            } catch (IOException e) {
                logger.error("Unexpected error {}", e);
                throw e;
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
        private ClientContext clientContext;
        private long accumulatedThroughput;
        private long elapsedTime;
        private String prefix;

        public WorkerThread(final Context context, final ClientContext clientContext) {
            this.context = context;
            this.clientContext = clientContext;
            this.prefix = "[WorkerThread-" + getId() + "]";
            this.accumulatedThroughput = 0;
            this.elapsedTime = 0;
        }

        public boolean isStopped() {
            return context.error() || (context.isStopped() && context.getSharedQueues().isDone());
        }

        private String getPrefix() {
            return prefix;
        }

        private String convertIntoWSDLInput(final String element) {
            final String [] lines = element.split("\n");
            String input = "";
            for (final String line : lines) {
                if (line.isEmpty()) continue;

                final Vector<String> v = StringHelper.splitString(SEPARATOR_FOR_INPUT, line);
                Assert.isTrue(v.size() == 3, "size check failed");

                if (input.isEmpty()) {
                    input += clientContext.getAuthKey();
                }
                input += "\n"
                      + v.get(0)
                      + BAR_SEPARATOR
                      + StringHelper.stripInvalidXMLCharacters(v.get(1))
                      + " "
                      // FIXME: removal of round brackets is workaround to avoid a bug in the address refiner
                      + StringHelper.removeRoundBrackets(StringHelper.stripInvalidXMLCharacters(v.get(2)));
//                logger.error("[{}] [{}]", v.get(1), v.get(2));
            }
//            logger.error("{}", input);

            return input;
        }

        @Override
        public void run() {
            try {
                execute();
            } catch(Exception e) {
                logger.error("Unexpected error {}", e);
                context.error(true);
            }
        }

        private void execute() throws Exception {
//            logger.error("{} start", getPrefix());
            final SharedQueues sharedQueues = context.getSharedQueues();
            Chunk chunk = null;
            final Worker worker = new Worker(clientContext.getClient());
            int retryCount = 0;
            while (! isStopped()) {
                while ((chunk = sharedQueues.todoQueue.poll()) == null) {
                    try {
//                        logger.error("{} {}", getPrefix(), context.toString());
                        Thread.sleep(TEN_MILLI_SECS);
                    } catch (InterruptedException e) {
                        logger.error("Unexpected error {}", e);
                        throw e;
                    }
                    if (isStopped()) break;
                }

                if (null == chunk) continue;

                Chunk doneChunk = new Chunk();
                long currThroughput = 0;
                long currElapsedTime = 0;

                boolean succeed = true;
                for (final String element : chunk) {
                    final String input = convertIntoWSDLInput(element);
                    if (! input.isEmpty()) {
                        try {
                            worker.run(input);
                        } catch (ConnectException | WebServiceIOException e) {
                            if (e.getMessage().contains("Operation timed out")) {
                                logger.error("Operation timed out happened. Let's retry it. Retry count={}", retryCount++);
                                succeed = false;
                            } else {
                                logger.error("{}\n{}", input, e);
                                throw e;
                            }
                        } catch (Exception e) {
                            logger.error("{}\n{}", input, e);
                            throw e;
                        }
                    }

                    if (succeed) {
                        saveResult(doneChunk, worker);
                        currElapsedTime += worker.getElapsedTime();
                        currThroughput += worker.getResult().split("\n").length;
                    } else {
                        break;
                    }
                }
                if (succeed) {
                    if (doneChunk.isEmpty()) {
                        sharedQueues.readyQueue.push(doneChunk);
                    } else {
                        sharedQueues.doneQueue.push(doneChunk);
                    }
                    accumulatedThroughput += currThroughput;
                    elapsedTime += currElapsedTime;
                } else {
                    sharedQueues.todoQueue.push(chunk);
                }
            }
        }

        public long getAccumulatedThroughput() {
            return accumulatedThroughput;
        }

        public long getElapsedTime() { return elapsedTime; }

        private String getIntegerStringWithZeroPadding(final int expectedSize, final String input) {
            final String s = getStringWithZeroPadding(expectedSize, input);
            assertCheckNumber(s);
            return s;
        }

        private String getStringWithZeroPadding(final int expectedSize, final String input) {
            Assert.isTrue(expectedSize >= input.length()
                    , "expected size should be bigger than or equal to input");
            final int sizeOfZeroPadding = expectedSize - input.length();
            final String s = (0 == sizeOfZeroPadding) ? input : String.format("%0" + sizeOfZeroPadding + "d", 0) + input;
            Assert.isTrue(s.length() == expectedSize, "Insufficient size");
            return s;
        }

        private void saveResult(Chunk chunk, final Worker worker) {
            String[] records = worker.getResult().split("\n");
            Assert.isTrue(records.length > 0, "empty result is not allowed");
//            logger.error("{} numRecords={}", getPrefix(), records.length);
            for (String record : records) {
                final Vector<String> v = StringHelper.splitString(BAR_SEPARATOR, record);
                final int expectedSizeOfWSDLResult = 37;
                if (v.size() != expectedSizeOfWSDLResult) {
                    // FIXME: Dummy key generation is workaround to avoid a bug in the address refiner
                    logger.error("Unexpected size of WSDL result\nresult=[{}]\n\nsplit result={}", record, v.toString());
                    final String output = v.get(0) + SEPARATOR_FOR_INPUT + generateDummyDMAPAddrID();
                    chunk.add(output);
                } else {
                    final String output = v.get(0) + SEPARATOR_FOR_INPUT + generateDMAPAddrID(v);
                    chunk.add(output);
                }
            }
        }

        private void assertCheckNumber(final String s) {
            Assert.isTrue(StringHelper.isNumeric(s), "Input should be numeric: [" + s + "]");
        }

        private String generateDMAPAddrID(final Vector<String> v) {
            /*
                ADDRESS ID

                Name        Bytes       Mapping index   Note
                주소구분코드     1
                시도구군코드     5            15
                법정동코드      5
                도로명코드      7            6             시군구코드(5)+도로명(7)
                지하여부       1            8
                건물본번       5            9
                건물부번       5            10
                동           8            21
                층           8            23
                호           4            22

             */

            final int expectedSizeOfIdentifier = 1;
            final String codeIdentifier = "1";

            final int idxCodeSido = 15;
            final int expectedSizeOfCodeSido = 10;
            // TODO: Handling exceptional cases (e.g. "11710XXX00")
            final String initialCodeSido = StringHelper.isNumeric(v.get(idxCodeSido)) ? v.get(idxCodeSido) : "";
            final String codeSido = getIntegerStringWithZeroPadding(expectedSizeOfCodeSido, initialCodeSido);

            final int idxCodeRD = 6;
            final int expectedSizeOfCodeRD = 7;
            final int startPosOfCodeRD = 5;
            final String initialCodeRD = v.get(idxCodeRD).length() == (startPosOfCodeRD + expectedSizeOfCodeRD) ?
                    v.get(idxCodeRD).substring(startPosOfCodeRD, v.get(idxCodeRD).length()) : "";
            final String codeRD = getIntegerStringWithZeroPadding(expectedSizeOfCodeRD, initialCodeRD);

            final int idxCodeBasement = 8;
            final int expectedSizeOfCodeBasement = 1;
            final String codeBasement = getIntegerStringWithZeroPadding(expectedSizeOfCodeBasement
                    , v.get(idxCodeBasement));
            Assert.isTrue(codeBasement.charAt(0) == '0' || codeBasement.charAt(0) == '1'
                    , "codeBasement should be either 0 or 1");

            final int idxCodeBD1 = 9;
            final int expectedSizeOfCodeBD1 = 5;
            final String codeBD1 = getIntegerStringWithZeroPadding(expectedSizeOfCodeBD1, v.get(idxCodeBD1));

            final int idxCodeBD2 = 10;
            final int expectedSizeOfCodeBD2 = 5;
            final String codeBD2 = getIntegerStringWithZeroPadding(expectedSizeOfCodeBD2, v.get(idxCodeBD2));

            final int idxCodeDong = 21;
            final int expectedSizeOfCodeDong = 8;
            // TODO: Handling exceptional cases (e.g. "A동")
            final String initialCodeDong = StringHelper.isNumeric(v.get(idxCodeDong)) ? v.get(idxCodeDong) : "";
            final String codeDong = getIntegerStringWithZeroPadding(expectedSizeOfCodeDong, initialCodeDong);

            final int idxCodeHo = 22;
            final int expectedSizeOfCodeHo = 4;
            // TODO: support numeric value extraction using regular expression (e.g. "B101")
            final String initialCodeHo = StringHelper.isNumeric(v.get(idxCodeHo)) ? v.get(idxCodeHo) : "";
            final String codeHo = getIntegerStringWithZeroPadding(expectedSizeOfCodeHo, initialCodeHo);

            final int idxCodeFloor = 23;
            final int expectedSizeOfCodeFloor = 8;
            // TODO: support numeric value extraction using regular expression (e.g. "지하1")
            final String initialCodeFloor = ! StringHelper.isNumeric(v.get(idxCodeFloor)) ? "" :
                    ! v.get(idxCodeFloor).isEmpty() ? v.get(idxCodeFloor) :
                            (initialCodeHo.isEmpty() ? "" : String.valueOf(Integer.parseInt(initialCodeHo) / 100));
            final String codeFloor = getIntegerStringWithZeroPadding(expectedSizeOfCodeFloor, initialCodeFloor);
/*
            logger.error("{}", record);
            logger.error("{}", codeSido);
            logger.error("{}", codeRD);
            logger.error("{}", codeBasement);
            logger.error("{}", codeBD1);
            logger.error("{}", codeBD2);
            logger.error("{}", codeDong);
            logger.error("{}", codeFloor);
            logger.error("{}", codeHo);
*/

            final String key = codeIdentifier
                    + codeSido
                    + codeRD
                    + codeBasement
                    + codeBD1
                    + codeBD2
                    + codeDong
                    + codeFloor
                    + codeHo;
            Assert.isTrue(key.length() == expectedSizeOfIdentifier
                    + expectedSizeOfCodeSido
                    + expectedSizeOfCodeRD
                    + expectedSizeOfCodeBasement
                    + expectedSizeOfCodeBD1
                    + expectedSizeOfCodeBD2
                    + expectedSizeOfCodeDong
                    + expectedSizeOfCodeFloor
                    + expectedSizeOfCodeHo, "key size is mismatched");

            return key;
        }

        private String generateDummyDMAPAddrID() {
            /*
                ADDRESS ID

                Name        Bytes       Mapping index   Note
                주소구분코드     1
                시도구군코드     5            15
                법정동코드      5
                도로명코드      7            6             시군구코드(5)+도로명(7)
                지하여부       1            8
                건물본번       5            9
                건물부번       5            10
                동           8            21
                층           8            23
                호           4            22

             */

            final int expectedSizeOfIdentifier = 1;
            final String codeIdentifier = "1";
            final int expectedSizeOfCodeSido = 10;
            final int expectedSizeOfCodeRD = 7;
            final int expectedSizeOfCodeBasement = 1;
            final int expectedSizeOfCodeBD1 = 5;
            final int expectedSizeOfCodeBD2 = 5;
            final int expectedSizeOfCodeDong = 8;
            final int expectedSizeOfCodeHo = 4;
            final int expectedSizeOfCodeFloor = 8;

            final String key = getIntegerStringWithZeroPadding(
                    expectedSizeOfIdentifier
                            + expectedSizeOfCodeSido
                            + expectedSizeOfCodeRD
                            + expectedSizeOfCodeBasement
                            + expectedSizeOfCodeBD1
                            + expectedSizeOfCodeBD2
                            + expectedSizeOfCodeDong
                            + expectedSizeOfCodeFloor
                            + expectedSizeOfCodeHo
                    , codeIdentifier);
            Assert.isTrue(key.length() == expectedSizeOfIdentifier
                    + expectedSizeOfCodeSido
                    + expectedSizeOfCodeRD
                    + expectedSizeOfCodeBasement
                    + expectedSizeOfCodeBD1
                    + expectedSizeOfCodeBD2
                    + expectedSizeOfCodeDong
                    + expectedSizeOfCodeFloor
                    + expectedSizeOfCodeHo, "key size is mismatched");

            return key;
        }

        public String toString() {
            return prefix + " accumulatedThroughput: " + getAccumulatedThroughput() + ", elapsedTime: " + getElapsedTime();
        }
    }

    public class ClientContext {
        public ClientContext(final int numThreads
                , final Client client
                , final ClientAppConfig config) {
            this.numThreads = numThreads;
            this.client = client;
            this.config = config;
        }
        public final int getNumThreads() { return numThreads; }
        public final Client getClient() { return client; }
        public final ClientAppConfig getClientAppConfig() { return config; }
        public final String getAuthKey() { return config.getAuthKey(); }

        private final int numThreads;
        private final Client client;
        private final ClientAppConfig config;
    }

    public class Context {
        public Context(final SharedQueues sharedQueues, final String inputFilename, final String outputFilename) {
            this.sharedQueues = sharedQueues;
            this.inputFilename = inputFilename;
            this.outputFilename = outputFilename;
            this.stopped = false;
            this.error = false;
        }

        public final SharedQueues getSharedQueues() { return sharedQueues; }
        public final String getInputFilename() { return inputFilename; }
        public final String getOutputFilename() { return outputFilename; }
        public boolean isStopped() { return stopped; }
        public void isStopped(boolean b) { stopped = b; }
        public boolean error() { return error; }
        public void error(boolean b) { error = b; }

        public String toString() {
            return "isStopped = " + (stopped ? "true" : "false")
                    + ", error = " + (error ? "true" : "false")
                    + ", " + sharedQueues.toString();
        }

        private final SharedQueues sharedQueues;
        private final String inputFilename;
        private final String outputFilename;
        private boolean stopped;
        private boolean error;
    }

    public void createWorkers(final Context context
            , final ClientContext clientContext
            , final ArrayList<Thread> threads
            , final ArrayList<WorkerThread> workerThreads) {
        final int numThread = clientContext.getNumThreads();

        for (int j = 0; j < numThread; ++j) {
            WorkerThread workerThread = new WorkerThread(context, clientContext);
            threads.add(workerThread);
            workerThreads.add(workerThread);
        }
    }

    public void run() {
        final SharedQueues sharedQueues = new SharedQueues();
        final Context context = new Context(sharedQueues, inputFilename, outputFilename);
//        final ClientContext devClientContext = new ClientContext(32, devClient, devClientAppConfig);
        final ClientContext productClientContext = new ClientContext(64, productClient, productClientAppConfig);

        final ArrayList<Thread> threads = new ArrayList<Thread>();
        final ArrayList<WorkerThread> workerThreads= new ArrayList<WorkerThread>();
        final Reader reader= new Reader(context);
        threads.add(reader);
        final Writer writer = new Writer(context);
        threads.add(writer);
//        createWorkers(context, devClientContext, threads, workerThreads);
        createWorkers(context, productClientContext, threads, workerThreads);

        try {
            reader.start();
            writer.start();

            for (WorkerThread workerThread : workerThreads) {
                workerThread.start();
            }

            for (final Thread thread : threads) {
                thread.join();
            }
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e);
            context.error(true);
        }

        long accumulatedThroughput = 0;
        long accumulatedElapsedTime = 0;

        int numOperatedThreads = 0;
        for (final WorkerThread workerThread : workerThreads) {
            if (workerThread.getElapsedTime() != 0) {
                numOperatedThreads++;
                accumulatedThroughput += workerThread.getAccumulatedThroughput();
                accumulatedElapsedTime += workerThread.getElapsedTime();
//                    logger.error(workerThread.toString());
            }
        }

        final double elapsedTime = accumulatedElapsedTime;
        logger.info("# of threads: {}, TPS: {}, Accumulated throughput: {}, Elapsed time: {}"
                , numOperatedThreads, accumulatedThroughput / (elapsedTime / 1000), accumulatedThroughput, elapsedTime);
    }

    public static void main(String [] args) {
        final boolean testMode = false;
        String inputFilename = null;
        String outputFilename = null;
        if (testMode) {
            System.err.println("************ TEST MODE **************");
            inputFilename = System.getProperty("user.dir") + "/data/input.txt";
            outputFilename = System.getProperty("user.dir") + "/data/output.txt";
        } else {
            final int expectedLength = 2;
            if (args.length < expectedLength) {
                System.err.println("Program failed because of insufficient arguments: <input file> <output file>"
                        + "[expectedLength: " + expectedLength + " != currentLength: " + args.length + "]");
                System.exit(1);
            }
            inputFilename = args[0];
            outputFilename = args[1];
        }
        if (inputFilename.equals(outputFilename)) {
            System.err.println("Program failed because the input file is equal to the output file");
            System.exit(1);
        }

        final WSDLClient WSDLClient = new WSDLClient(inputFilename, outputFilename);
        WSDLClient.run();
    }
}
