package dk.dbc.rawrepo.writer;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.MarcXchangeV1Writer;

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

@Provider
@Produces(MediaType.APPLICATION_XML)
public class MarcRecordXMLMessageBodyWriter implements MessageBodyWriter<MarcRecord> {

    @Override
    public boolean isWriteable(Class<?> type, Type genericType, Annotation[] annotations, MediaType mediaType) {
        return genericType == MarcRecord.class;
    }

    @Override
    public long getSize(MarcRecord marcRecord, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType) {
        return 0;
    }

    @Override
    public void writeTo(MarcRecord marcRecord, Class<?> aClass, Type type, Annotation[] annotations, MediaType mediaType,
                        MultivaluedMap<String, Object> multivaluedMap, OutputStream outputStream) throws IOException, WebApplicationException {
        MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

        outputStream.write(marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));
    }
}
