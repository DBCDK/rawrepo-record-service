package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.DanMarc2Charset;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.Iso2709MarcRecordWriter;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.rawrepo.dto.ContentDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.service.RecordObjectMapper;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static dk.dbc.marc.writer.MarcXchangeV1Writer.Property.ADD_XML_DECLARATION;

public class RecordByteWriter {
    private final OutputStream outputStream;
    private final OutputFormat outputFormat;
    private final Charset charset;

    private final JSONBContext jsonbContext = new JSONBContext();
    private final DanMarc2LineFormatWriter danMarc2LineFormatWriter = new DanMarc2LineFormatWriter();
    private final Iso2709MarcRecordWriter iso2709Writer = new Iso2709MarcRecordWriter();
    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();
    private static final String COLLECTION_FOOTER_XML = "</collection>";
    private static final String COLLECTION_HEADER_XML = "<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>";

    public RecordByteWriter(OutputStream outputStream, Params params) {
        this.outputStream = outputStream;
        this.outputFormat = OutputFormat.fromString(params.getOutputFormat());
        this.charset = "DANMARC2".equalsIgnoreCase(params.getOutputEncoding()) ? new DanMarc2Charset() : Charset.forName(params.getOutputEncoding());

        // marcXchangeV1Writer is used for writing collections. This means the output already has XML declaration so
        // we don't want to write that in every record
        this.marcXchangeV1Writer.setProperty(ADD_XML_DECLARATION, false);
    }

    public void writeHeader() throws IOException {
        if (outputFormat == OutputFormat.XML) {
            final String xmlHeader = "<?xml version='1.0' encoding='" + charset.name() + "'?>\n";

            outputStream.write(xmlHeader.getBytes(charset));
            outputStream.write(COLLECTION_HEADER_XML.getBytes(charset));
        }
    }

    public void writeFooter() throws IOException {
        if (outputFormat == OutputFormat.XML) {
            outputStream.write(COLLECTION_FOOTER_XML.getBytes(charset));
        }
    }

    public void write(byte[] data) throws IOException, MarcReaderException, JSONBException, MarcWriterException, SAXException {
        final MarcRecord marcRecord;
        final byte[] recordBytes;

        switch (outputFormat) {
            case JSON:
                final MarcRecord recordJSON = RecordObjectMapper.contentToMarcRecord(data);
                final ContentDTO contentDTO = RecordDTOMapper.contentToDTO(recordJSON);
                recordBytes = jsonbContext.marshall(contentDTO).getBytes(charset);
                synchronized (this) {
                    outputStream.write(recordBytes);
                    outputStream.write("\n".getBytes(charset));
                }
                break;
            case LINE:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                synchronized (this) {
                    outputStream.write(danMarc2LineFormatWriter.write(marcRecord, charset));
                }
                break;
            case LINE_XML:
                synchronized (this) {
                    outputStream.write(data);
                    outputStream.write("\n".getBytes(charset));
                }
                break;
            case XML:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                recordBytes = marcXchangeV1Writer.write(marcRecord, charset);
                synchronized (this) {
                    outputStream.write(recordBytes);
                    outputStream.write("\n".getBytes(charset));
                }
                break;
            case ISO:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                recordBytes = iso2709Writer.write(marcRecord, charset);
                synchronized (this) {
                    outputStream.write(recordBytes);
                }
                break;
        }
    }

}
