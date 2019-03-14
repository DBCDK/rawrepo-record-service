/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.Iso2709Writer;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.rawrepo.dto.ContentDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.service.RecordObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

import static dk.dbc.marc.writer.MarcXchangeV1Writer.Property.ADD_XML_DECLARATION;

public class RecordByteWriter {
    private OutputStream outputStream;
    private Params params;
    private OutputFormat outputFormat;

    private final JSONBContext jsonbContext = new JSONBContext();
    private final DanMarc2LineFormatWriter danMarc2LineFormatWriter = new DanMarc2LineFormatWriter();
    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();
    private final Iso2709Writer iso2709Writer = new Iso2709Writer();

    public RecordByteWriter(OutputStream outputStream, Params params) {
        this.outputStream = outputStream;
        this.params = params;
        this.outputFormat = OutputFormat.fromString(this.params.getOutputFormat());

        // marcXchangeV1Writer is used for writing collections. This means the output already has XML declaration so
        // we don't want to write that in every record
        marcXchangeV1Writer.setProperty(ADD_XML_DECLARATION, false);
    }

    public void writeHeader() throws IOException {
        if (outputFormat == OutputFormat.XML) {
            String xmlHeader = "<?xml version='1.0' encoding='" +  params.getOutputEncoding() +"'?>\n";
            String collectionHeader = "<collection xmlns='info:lc/xmlns/marcxchange-v1' xmlns:xsi='http://www.w3.org/2001/XMLSchema-instance' xsi:schemaLocation='info:lc/xmlns/marcxchange-v1 http://www.loc.gov/standards/iso25577/marcxchange-1-1.xsd'>";

            outputStream.write(xmlHeader.getBytes());
            outputStream.write(collectionHeader.getBytes());
        }
    }

    public void writeFooter() throws IOException {
        if (outputFormat == OutputFormat.XML) {
            String collectionFooter = "</collection>";

            outputStream.write(collectionFooter.getBytes());
        }
    }

    public void write(byte[] data) throws IOException, MarcReaderException, JSONBException, MarcWriterException {
        MarcRecord marcRecord;
        byte[] recordBytes;

        switch (outputFormat) {
            case JSON:
                MarcRecord recordJSON = RecordObjectMapper.contentToMarcRecord(data);
                ContentDTO contentDTO = RecordDTOMapper.contentToDTO(recordJSON);
                recordBytes = jsonbContext.marshall(contentDTO).getBytes(params.getOutputEncoding());
                synchronized (this) {
                    outputStream.write(recordBytes);
                    outputStream.write("\n".getBytes());
                }
                break;
            case LINE:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                synchronized (this) {
                    outputStream.write(danMarc2LineFormatWriter.write(marcRecord, Charset.forName(params.getOutputEncoding())));
                }
                break;
            case XML:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                recordBytes = marcXchangeV1Writer.write(marcRecord, Charset.forName(params.getOutputEncoding()));
                synchronized (this) {
                    outputStream.write(recordBytes);
                    outputStream.write("\n".getBytes());
                }
                break;
            case ISO:
                marcRecord = RecordObjectMapper.contentToMarcRecord(data);
                synchronized (this) {
                    outputStream.write(iso2709Writer.write(marcRecord, Charset.forName(params.getOutputEncoding())));
                }
                break;
        }
    }

}
