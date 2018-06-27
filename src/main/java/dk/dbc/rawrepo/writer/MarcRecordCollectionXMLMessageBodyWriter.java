package dk.dbc.rawrepo.writer;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.MultivaluedMap;
import javax.ws.rs.ext.MessageBodyWriter;
import javax.ws.rs.ext.Provider;
import java.io.IOException;
import java.io.OutputStream;
import java.lang.annotation.Annotation;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashSet;

@Provider
@Produces(MediaType.APPLICATION_XML)
public class MarcRecordCollectionXMLMessageBodyWriter implements MessageBodyWriter<Collection<MarcRecord>> {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(MarcRecordCollectionXMLMessageBodyWriter.class);

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        // TODO expand check to include generic type
        // MessageBodyWriter not found for media type=application/xml,
        // type=class java.util.HashSet,
        // genericType=java.util.Collection<dk.dbc.marc.binding.MarcRecord>.

        return type == HashSet.class;
    }

    @Override
    public long getSize(Collection<MarcRecord> marcRecords, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return -1;
    }

    @Override
    public void writeTo(Collection<MarcRecord> marcRecords, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream) throws IOException, WebApplicationException {
        MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

        outputStream.write(marcXchangeV1Writer.writeCollection(marcRecords, Charset.forName("UTF-8")));
    }
}
