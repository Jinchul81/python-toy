import com.skt.dbp.config.DevClientAppConfig;
import com.skt.dbp.config.ProductClientAppConfig;
import com.skt.dbp.wsdl.Client;
import com.skt.dbp.config.ClientAppConfig;
import com.skt.dbp.config.Property;

/**
 * Created by jinchulkim on 2017. 7. 13..
 */
public abstract class AbstractLoadTest {
    protected final Client client;
    protected final String authKey = Property.authKey;
    protected final int TEN_MILLI_SECS = 1000;

    public AbstractLoadTest() {
        ProductClientAppConfig config = new ProductClientAppConfig();
        this.client = config.convertClient(config.marshaller());
    }


    abstract public void run();
}
