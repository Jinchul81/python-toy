package com.skt.dbp.config;

/**
 * Created by jinchulkim on 2017. 8. 24..
 */
public class ProductClientAppConfig extends ClientAppConfig {
    public ProductClientAppConfig() {
        super(ProductProperty.uri, ProductProperty.authKey);
    }
}