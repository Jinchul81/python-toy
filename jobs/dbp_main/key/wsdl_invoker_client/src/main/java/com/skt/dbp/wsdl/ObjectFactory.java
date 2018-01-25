package com.skt.dbp.wsdl;

import javax.xml.bind.annotation.XmlRegistry;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
@XmlRegistry
public class ObjectFactory {
    public ObjectFactory() {
    }

    public GetConvertRequest createGetConvertRequest() {
        return new GetConvertRequest();
    }

    public GetConvertResponse createGetConvertResponse() {
        return new GetConvertResponse();
    }

    public String createGetConvertReturn() {
        return new String();
    }
}
