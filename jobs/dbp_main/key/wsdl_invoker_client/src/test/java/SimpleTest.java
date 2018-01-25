import com.skt.dbp.utility.Worker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
public class SimpleTest extends AbstractLoadTest {
    private static final Logger logger = LoggerFactory.getLogger(SimpleTest.class);

    @Override
    public void run() {
            String input = super.authKey;
            //input += "\n1|경기도 포천시 소홀읍 호국로 511-80";
            //input += "\n1|경기도 성남시 분당구 수내동 23-1 101호";
            //input += "\n1|서울 송파구 신천동 11번지 잠실아이스페이스(한화손해보험) 빌딩 805호]\n";
            //input += "\n1|서울 송파구 신천동 11번지 잠실아이스페이스 한화손해보험 빌딩 805호]\n";
            input += "\n1|서울 서대문구 남가좌1동 104-27번지 금성빌라 101가호|\n";
            //input += "\n1|서울 서대문구 남가좌1동 104-27번지 금성빌라 201가호|\n";
            //input += "\n1|서울 송파구 신천동 11번지 한화손해보험 빌딩 805호]\n";
            //input += "\n1|광주 남구 양림동 280 204동 2001호";

            final Worker worker = new Worker(super.client);
            try {
                worker.run(input);
            } catch (Exception e) {
                logger.error("Unexecpted error {}", e);
                return;
            }
            final String result = worker.getResult();
            final String[] rows = result.split("\n");
            StringBuilder message = new StringBuilder("\n");
            for (String row : rows) {
                final String[] cols = row.split("\\|");
                int idx = 0;
                for (String col : cols) {
                    message.append("[").append(idx).append("] = [").append(col).append("]\n");
                    idx++;
                }
            }
            logger.error(message.toString());
    }

    public static void main(String [] args) {
        final SimpleTest test = new SimpleTest();
        test.run();
    }
}

//41650 25022101450002010785