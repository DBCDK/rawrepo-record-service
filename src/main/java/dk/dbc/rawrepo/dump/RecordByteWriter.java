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
import dk.dbc.rawrepo.dto.ContentDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.service.RecordObjectMapper;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class RecordByteWriter {
    private OutputStream outputStream;
    private Params params;

    private final JSONBContext jsonbContext = new JSONBContext();
    private final DanMarc2LineFormatWriter danMarc2LineFormatWriter = new DanMarc2LineFormatWriter();
    private final Iso2709Writer iso2709Writer = new Iso2709Writer();

    public RecordByteWriter(OutputStream outputStream, Params params) {
        this.outputStream = outputStream;
        this.params = params;
    }

    public void write(byte[] data) throws IOException, MarcReaderException, JSONBException, MarcWriterException {
        switch (OutputFormat.fromString(params.getOutputFormat())) {
            case JSON:
                MarcRecord recordJSON = RecordObjectMapper.contentToMarcRecord(data);
                ContentDTO contentDTO = RecordDTOMapper.contentToDTO(recordJSON);
                synchronized (this) {
                    outputStream.write(jsonbContext.marshall(contentDTO).getBytes(params.getOutputEncoding()));
                    outputStream.write("\n".getBytes());
                }
                break;
            case LINE:
                MarcRecord recordLine = RecordObjectMapper.contentToMarcRecord(data);
                synchronized (this) {
                    outputStream.write(danMarc2LineFormatWriter.write(recordLine, Charset.forName(params.getOutputEncoding())));
                }
                break;
            case XML:
                synchronized (this) {
                    outputStream.write(data);
                    outputStream.write("\n".getBytes());
                }
                break;
            case ISO:
                MarcRecord recordLineISO = RecordObjectMapper.contentToMarcRecord(data);
                synchronized (this) {
                    outputStream.write(iso2709Writer.write(recordLineISO, Charset.forName(params.getOutputEncoding())));
                }
                break;
        }
    }

}
