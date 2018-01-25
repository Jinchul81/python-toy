

package com.skt.dbp.wsdl;

import javax.xml.bind.annotation.*;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */


@XmlRootElement(name = "getConvertResponse")
@XmlAccessorType(XmlAccessType.FIELD)
public class GetConvertResponse {
    @XmlElement(required = true)
    String getConvertReturn;

    public String getGetConvertReturn() {
        return getConvertReturn;
    }

    public void setGetConvertReturn(final String value) {
        this.getConvertReturn = value;
    }
}
