// https://stackoverflow.com/questions/6895486/jaxb-need-namespace-prefix-to-all-the-elements
@javax.xml.bind.annotation.XmlSchema(
        namespace = "your URL",
        elementFormDefault = javax.xml.bind.annotation.XmlNsForm.QUALIFIED,
        xmlns = {
                @javax.xml.bind.annotation.XmlNs(prefix="ns1", namespaceURI="your URL")
        }
)
package com.skt.dbp.wsdl;
