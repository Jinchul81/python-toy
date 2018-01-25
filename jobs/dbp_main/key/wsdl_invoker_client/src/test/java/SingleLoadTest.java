import com.skt.dbp.utility.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
public class SingleLoadTest extends AbstractLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(SingleLoadTest.class);

    @Override
    public void run() {
        final int [] numCountArray = new int[]{1, 10, 100, 500, 1000, 5000, 10000};

        for (int i = 0; i < numCountArray.length; ++i) {
            final int numCount = numCountArray[i];
            final Worker worker = new Worker(super.client);
            for (int j = 0; j < numCount; ++j) {
                String input = authKey;
                for (int k = 1; k <= numCount; k++) {
                    input += "\n" + k + "|경기도 성남시 분당구 수내동 23-1 " + k + "호";
                }
                try {
                    worker.run(input);
                    return;
                } catch (Exception e) {
                    logger.error("Unexpected error {}", e);
                }
                //logger.error(worker.getResult());
                logger.error("NumCount: {}, Elapsed time for total: {} ms, Elapsed time for unit: {} ms", numCount, worker.getElapsedTime(), worker.getElapsedTime() / numCount);
            }
        }
    }

    public static void main(String [] args) {
        final SingleLoadTest test = new SingleLoadTest();
        test.run();
    }
}
