package dk.dbc.rawrepo.service;

import dk.dbc.marc.DanMarc2Charset;
import dk.dbc.marc.DanMarc2LineFormatConcatWriter;
import dk.dbc.marc.LineFormatConcatWriter;
import dk.dbc.marc.MarcFormatDeducer;
import dk.dbc.marc.RecordFormat;
import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.DanMarc2LineFormatReader;
import dk.dbc.marc.reader.Iso2709Reader;
import dk.dbc.marc.reader.JsonLineReader;
import dk.dbc.marc.reader.LineFormatReader;
import dk.dbc.marc.reader.MarcReader;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.reader.MarcXmlReader;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.Iso2709MarcRecordWriter;
import dk.dbc.marc.writer.JsonLineWriter;
import dk.dbc.marc.writer.LineFormatWriter;
import dk.dbc.marc.writer.MarcWriter;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;


@Interceptors({StopwatchInterceptor.class})
@Stateless
@Path("api")
public class MarcConverterService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(MarcConverterService.class);
    private static final int PUSHBACK_BUFFER_SIZE = 1000;
    private static final boolean INCLUDE_WHITESPACE_PADDING = false;


    private Charset stringToCharset(String value) throws IllegalArgumentException {
        if ("danmarc2".equalsIgnoreCase(value)) {
            return new DanMarc2Charset();
        } else if ("latin-1".equalsIgnoreCase(value) || "latin1".equalsIgnoreCase(value)) {
            return StandardCharsets.ISO_8859_1;
        } else {
            try {
                return Charset.forName(value);
            } catch (UnsupportedCharsetException ex) {
                // The message of UnsupportedCharsetException is just the value you supplied
                throw new IllegalArgumentException(value + " is not a valid charset");
            }
        }
    }

    @POST
    @Path("v1/mconv")
    @Timed
    public Response convert(String recordData,
                            @QueryParam("output-format") String outputFormatString,
                            @DefaultValue("UTF-8") @QueryParam("input-encoding") String inputEncodingString,
                            @DefaultValue("UTF-8") @QueryParam("output-encoding") String outputEncodingString,
                            @DefaultValue("false") @QueryParam("as-collection") boolean asCollection,
                            @DefaultValue("false") @QueryParam("include-leader") boolean includeLeader,
                            @DefaultValue("false") @QueryParam("include-padding") boolean includeWhitespacePadding) {
        try {
            final Charset outputEncoding = stringToCharset(outputEncodingString);
            final Charset inputEncoding = stringToCharset(inputEncodingString);

            if (outputFormatString == null) {
                throw new IllegalArgumentException("output-format must be set");
            }

            final RecordFormat outputFormat = RecordFormat.valueOf(outputFormatString);

            final StreamingOutput output = out -> {
                try {
                    InputStream targetStream = new ByteArrayInputStream(recordData.getBytes());
                    PushbackInputStream is = new PushbackInputStream(targetStream, PUSHBACK_BUFFER_SIZE);

                    final MarcReader marcRecordReader = getMarcReader(is, inputEncoding);
                    MarcRecord marcRecord = marcRecordReader.read();

                    if (marcRecord == null) {
                        throw new IllegalArgumentException("Unknown input format");
                    }
                    final MarcWriter marcWriter = getMarcWriter(marcRecord, outputFormat, includeLeader, includeWhitespacePadding);

                    List<MarcRecord> recordBuffer = null;
                    if (asCollection) {
                        if (!marcWriter.canOutputCollection()) {
                            throw new IllegalArgumentException("Output format " + outputFormat + " does not support collections");
                        }
                        recordBuffer = new ArrayList<>();
                    }

                    while (marcRecord != null) {
                        if (asCollection) {
                            recordBuffer.add(marcRecord);
                        } else {
                            out.write(marcWriter.write(marcRecord, outputEncoding));
                        }
                        marcRecord = marcRecordReader.read();
                    }

                    if (recordBuffer != null) {
                        out.write(marcWriter.writeCollection(recordBuffer, outputEncoding));
                    }
                } catch (MarcWriterException | IOException ex) {
                    LOGGER.error("Exception during convert", ex);
                    throw new WebApplicationException("Caught exception during write", ex);
                } catch (MarcReaderException ex) {
                    throw new IllegalArgumentException(ex);
                }
            };

            return Response.ok(output, recordFormatToContentType(outputFormat)).build();
        } catch (WebApplicationException ex) {
            LOGGER.error("Exception during convert", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (IllegalArgumentException ex) {
            LOGGER.info("IllegalArgumentException", ex);
            return Response.status(Response.Status.BAD_REQUEST).entity(ex.getMessage()).build();
        }
    }

    private String recordFormatToContentType(RecordFormat recordFormat) {
        final String mediaType;
        switch(recordFormat){
            case JSONL:
                mediaType= MediaType.APPLICATION_JSON;
                break;
            case MARCXCHANGE:
                mediaType = MediaType.APPLICATION_XML;
                break;
            default:
                mediaType = MediaType.TEXT_PLAIN;
                break;
        }

        return mediaType;
    }

    private MarcReader getMarcReader(PushbackInputStream is, Charset encoding) throws MarcReaderException {
        final MarcFormatDeducer marcFormatDeducer = new MarcFormatDeducer(PUSHBACK_BUFFER_SIZE);

        Charset sampleEncoding = encoding;
        if (!(encoding.name().equals("UTF-8"))) {
            // Don't complicate the format deduction
            // by introducing the DanMarc2 charset
            // into the mix.
            sampleEncoding = StandardCharsets.ISO_8859_1;
        }
        final MarcFormatDeducer.FORMAT format =
                marcFormatDeducer.deduce(is, sampleEncoding);

        if (format == MarcFormatDeducer.FORMAT.LINE
                && encoding instanceof DanMarc2Charset) {
            // For line format we need a special
            // variant of the DanMarc2 charset.
            encoding = new DanMarc2Charset(DanMarc2Charset.Variant.LINE_FORMAT);
        }

        switch (format) {
            case JSONL:
                return new JsonLineReader(is, encoding);
            case LINE:
                return new LineFormatReader(is, encoding)
                        .setProperty(LineFormatReader.Property.INCLUDE_WHITESPACE_PADDING, INCLUDE_WHITESPACE_PADDING);
            case DANMARC2_LINE:
                return new DanMarc2LineFormatReader(is, encoding);
            case MARCXCHANGE:
                return new MarcXchangeV1Reader(is, encoding);
            case MARCXML:
                return new MarcXmlReader(is, encoding);
            default:
                return new Iso2709Reader(is, encoding);
        }
    }

    private MarcWriter getMarcWriter(MarcRecord marcRecord, RecordFormat outputFormat, boolean includeLeader,
                                     boolean includeWhitespacePadding) {
        switch (outputFormat) {
            case LINE: // pass-through
            case LINE_CONCAT:
                return getLineFormatWriterVariant(marcRecord, outputFormat, includeLeader, includeWhitespacePadding);
            case ISO:
                return new Iso2709MarcRecordWriter();
            case JSONL:
                return new JsonLineWriter();
            case MARCXCHANGE:
                return getMarcXchangeWriter();
            default:
                throw new IllegalStateException("Unhandled format: Should not happen");
        }
    }

    private MarcWriter getLineFormatWriterVariant(MarcRecord marcRecord, RecordFormat outputFormat,
                                                  boolean includeLeader, boolean includeWhitespacePadding) {
        if (isDanMarc2(marcRecord)) {
            return outputFormat == RecordFormat.LINE ? new DanMarc2LineFormatWriter()
                    : new DanMarc2LineFormatConcatWriter();
        }
        final LineFormatWriter lineFormatWriter = outputFormat == RecordFormat.LINE
                ? new LineFormatWriter() : new LineFormatConcatWriter();

        return lineFormatWriter
                .setProperty(LineFormatWriter.Property.INCLUDE_LEADER,
                        includeLeader)
                .setProperty(LineFormatWriter.Property.INCLUDE_WHITESPACE_PADDING,
                        includeWhitespacePadding);
    }

    private MarcWriter getMarcXchangeWriter() {
        final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();
        marcXchangeV1Writer.setProperty(MarcXchangeV1Writer.Property.ADD_XML_DECLARATION, false);
        return marcXchangeV1Writer;
    }

    private static boolean isDanMarc2(MarcRecord marcRecord) {
        final List<Field> fields = marcRecord.getFields();
        return !fields.isEmpty() && fields.get(0) instanceof DataField;
    }

}
