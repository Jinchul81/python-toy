package com.skt.dbp.wsdl;

import com.skt.dbp.utility.JAXBHelper;
import com.skt.dbp.wsdl.GetConvertRequest;
import com.skt.dbp.wsdl.GetConvertResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.ws.client.core.WebServiceTemplate;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
public class Client extends WebServiceTemplate {
    //private static final Logger logger = LoggerFactory.getLogger(Client.class);

    public GetConvertResponse getByInput(final String input) {
        final GetConvertRequest request = new GetConvertRequest();
        request.setIn(input);

        //logger.debug(JAXBHelper.getXML(request));
        final GetConvertResponse response = (GetConvertResponse) marshalSendAndReceive(request);
        //logger.debug(JAXBHelper.getXML(response));
        return response;
    }
}
