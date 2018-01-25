package com.skt.dbp.config;

import com.skt.dbp.wsdl.Client;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.oxm.jaxb.Jaxb2Marshaller;

/**
 * Created by jinchulkim on 2017. 8. 24..
 */
public class ClientAppConfig {
    protected final String uri;
    protected final String authKey;

    public ClientAppConfig(final String uri, final String authKey) {
        this.uri = uri;
        this.authKey = authKey;
    }

    public Jaxb2Marshaller marshaller() {
        final Jaxb2Marshaller marshaller = new Jaxb2Marshaller();
        marshaller.setContextPath("com.skt.dbp.wsdl");
        return marshaller;
    }

    public Client convertClient(final Jaxb2Marshaller marshaller) {
        final Client client = new Client();
        client.setDefaultUri(uri);
        client.setMarshaller(marshaller);
        client.setUnmarshaller(marshaller);
        return client;
    }

    public String getAuthKey() {
        return authKey;
    }
}
