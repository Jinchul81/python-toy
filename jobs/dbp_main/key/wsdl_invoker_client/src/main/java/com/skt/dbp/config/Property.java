package com.skt.dbp.config;

/**
 * Created by jinchulkim on 2017. 8. 3..
 */

class DevProperty {
    public static String uri = "your uri";
    public static String authKey = "your auth key";
}

class ProductProperty {
    public static String uri = "your uri";
    public static String authKey = "your auth key";
}

public class Property {
    public static String uri = ProductProperty.uri;
    public static String authKey = ProductProperty.authKey;
}

