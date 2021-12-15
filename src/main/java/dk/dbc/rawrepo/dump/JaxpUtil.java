package dk.dbc.rawrepo.dump;

import org.w3c.dom.Document;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.ByteArrayInputStream;
import java.io.IOException;

/**
 * Helper class stolen from DataIO commons/utils/lang/src/main/java/dk/dbc/dataio/commons/utils/lang/JaxpUtil.java
 * <p>
 * JaxpUtil - utility class providing helper methods for XML processing safe for use in
 * a multi-threaded environment
 * <p>
 * This class ensures thread safety by using thread local variables for the DocumentBuilderFactory
 * and TransformerFactory classes used internally by the methods. If not handled carefully in
 * environments using thread pools with long lived threads this might cause memory leak problems
 * so make sure to use appropriate memory analysis tools to verify correct behaviour.
 * </p>
 */
public class JaxpUtil {

    // SonarLint S1118 - Utility classes should not have public constructors
    private JaxpUtil() {

    }

    /**
     * Thread local variable used to give each thread its own DocumentBuilderFactory (since it is not thread-safe)
     */
    private static final ThreadLocal<DocumentBuilderFactory> documentBuilderFactory = ThreadLocal.withInitial(() -> {
        DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
        dbf.setNamespaceAware(true);
        dbf.setValidating(false);
        return dbf;
    });

    /**
     * Converts a byte array to its XML document representation
     *
     * @param bytes input bytes
     * @return document representation
     * @throws NullPointerException  if given null-valued bytes argument
     * @throws IOException           If any IO errors occur.
     * @throws SAXException          If any parse errors occur
     * @throws IllegalStateException if a DocumentBuilder cannot be created
     */
    public static Document toDocument(byte[] bytes) throws IOException, SAXException {
        try {
            final ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(bytes);
            final DocumentBuilderFactory documentBuilderFactory = JaxpUtil.documentBuilderFactory.get();
            final DocumentBuilder documentBuilder = documentBuilderFactory.newDocumentBuilder();
            return documentBuilder.parse(byteArrayInputStream);
        } catch (ParserConfigurationException e) {
            throw new IllegalStateException(e);
        }
    }

}
