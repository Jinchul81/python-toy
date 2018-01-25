package com.skt.dbp.utility;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.xml.bind.JAXBContext;
import javax.xml.bind.Marshaller;
import java.io.StringWriter;

/**
 * Created by jinchulkim on 2017. 7. 12..
 */
public class JAXBHelper {
    private static final Logger logger = LoggerFactory.getLogger(JAXBHelper.class);

    public static <T> String getXML(final T obj) {
        try {
            final JAXBContext context = JAXBContext.newInstance(obj.getClass());
            final Marshaller m = context.createMarshaller();
            m.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT, true);
            final StringWriter sw = new StringWriter();
            m.marshal(obj, sw);

            return sw.toString();
        } catch (final Exception e) {
            logger.error("Unexpected error: {}", e);
        }

        return "";
    }
}
