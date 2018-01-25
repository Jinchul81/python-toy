package com.skt.dbp.config;

/**
 * Created by jinchulkim on 2017. 8. 24..
 */
public class DevClientAppConfig extends ClientAppConfig {
    public DevClientAppConfig() {
        super(DevProperty.uri, DevProperty.authKey);
    }
}