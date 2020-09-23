package nl.pdok.delta.download.util;

import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;

public class XmlReaderUtil {


    public static String getTagContentAsXML(XMLStreamReader xmlr) throws XMLStreamException, IOException {

        StringWriter out = new StringWriter();

        String tagLocalName = xmlr.getLocalName();
        while (xmlr.hasNext()) {
            xmlr.next();
            if (xmlr.getEventType() == XMLStreamConstants.END_ELEMENT && tagLocalName.equals(xmlr.getLocalName())) {
                break;
            }

            switch (xmlr.getEventType()) {
                case XMLStreamConstants.START_ELEMENT:
                    out.write("<");
                   writeName(xmlr, out);
                   writeNamespaces(xmlr, out);
                   writeAttributes(xmlr, out);
                    out.write(">");
                    break;
                case XMLStreamConstants.END_ELEMENT:
                    out.write("</");
                   writeName(xmlr, out);
                    out.write(">");
                    break;
                case XMLStreamConstants.SPACE:
                case XMLStreamConstants.CHARACTERS:
                    int start = xmlr.getTextStart();
                    int length = xmlr.getTextLength();
                    out.write(new String(xmlr.getTextCharacters(),
                            start,
                            length));
                    break;
                case XMLStreamConstants.PROCESSING_INSTRUCTION:
                    out.write("<?");
                    if (xmlr.hasText())
                        out.write(xmlr.getText());
                    out.write("?>");
                    break;
                case XMLStreamConstants.CDATA:
                    out.write("<![CDATA[");
                    start = xmlr.getTextStart();
                    length = xmlr.getTextLength();
                    out.write(new String(xmlr.getTextCharacters(),
                            start,
                            length));
                    out.write("]]>");
                    break;
                case XMLStreamConstants.COMMENT:
                    out.write("<!--");
                    if (xmlr.hasText())
                        out.write(xmlr.getText());
                    out.write("-->");
                    break;
                case XMLStreamConstants.ENTITY_REFERENCE:
                    out.write(xmlr.getLocalName() + "=");
                    if (xmlr.hasText())
                        out.write("[" + xmlr.getText() + "]");
                    break;
                case XMLStreamConstants.START_DOCUMENT:
                    out.write("<?xml");
                    out.write(" version=\"" + xmlr.getVersion() + "\"");
                    out.write(" encoding=\"" + xmlr.getCharacterEncodingScheme() + "\"");
                    if (xmlr.isStandalone())
                        out.write(" standalone=\"yes\"");
                    else
                        out.write(" standalone=\"no\"");
                    out.write("?>");
                    break;
            }


        }
        // empty tag
        if (!xmlr.hasNext()) {
            throw new XMLStreamException("End element for " + tagLocalName + " not found");
        }

        return out.toString().replaceAll("\n", "").replaceAll("'", "''");
    }


    private static void writeName(XMLStreamReader xmlr, Writer out) throws IOException {
        if (xmlr.hasName()) {
            String prefix = xmlr.getPrefix();
            String uri = xmlr.getNamespaceURI();
            if (!"".equals(prefix)) {

            }
            String localName = xmlr.getLocalName();
           writeName(prefix, uri, localName, out);
        }
    }

    private static void writeName(String prefix,
                           String uri,
                           String localName, Writer out) throws IOException {
        //  if (uri != null && !("".equals(uri)) ) out.write("[""+uri+""]:");
        if (prefix != null && !"".equals(prefix)) out.write(prefix + ":");
        if (localName != null) out.write(localName);
    }

    private static void writeAttributes(XMLStreamReader xmlr, Writer out) throws IOException {
        for (int i = 0; i < xmlr.getAttributeCount(); i++) {
           writeAttribute(xmlr, i, out);
        }
    }

    private static void writeAttribute(XMLStreamReader xmlr, int index, Writer out) throws IOException {
        String prefix = xmlr.getAttributePrefix(index);
        String namespace = xmlr.getAttributeNamespace(index);
        String localName = xmlr.getAttributeLocalName(index);
        String value = xmlr.getAttributeValue(index);
        out.write(" ");
        writeName(prefix, namespace, localName, out);
        out.write("=\"" + value + "\"");
    }

    private static void writeNamespaces(XMLStreamReader xmlr, Writer out) throws IOException {
        for (int i = 0; i < xmlr.getNamespaceCount(); i++) {
           writeNamespace(xmlr, i, out);
        }
    }

    private static void writeNamespace(XMLStreamReader xmlr, int index, Writer out) throws IOException {
        String prefix = xmlr.getNamespacePrefix(index);
        String uri = xmlr.getNamespaceURI(index);
        out.write(" ");
        if (prefix == null)
            out.write("xmlns=\"" + uri + "\"");
        else
            out.write("xmlns:" + prefix + "=\"" + uri + "\"");
    }
}
