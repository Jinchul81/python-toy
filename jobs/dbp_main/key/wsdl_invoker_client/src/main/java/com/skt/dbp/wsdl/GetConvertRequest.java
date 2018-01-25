package com.skt.dbp.wsdl;

import javax.xml.bind.annotation.*;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */

@XmlRootElement(name = "getConvert")
@XmlAccessorType(XmlAccessType.FIELD)
public class GetConvertRequest {
    @XmlElement(required = true)
    String in;

    public String getIn() {
        return in;
    }

    public void setIn(String in) {
        this.in = in;
    }
}
