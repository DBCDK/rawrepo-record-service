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
    private final DanMarc2LineFormatWriter lineFormatWriter = new DanMarc2LineFormatWriter();

    public RecordByteWriter(OutputStream outputStream, Params params) {
        this.outputStream = outputStream;
        this.params = params;
    }

    public void write(byte[] data) throws IOException, MarcReaderException, JSONBException, MarcWriterException {
        synchronized (this) {
            switch (OutputFormat.fromString(params.getOutputFormat())) {
                case JSON:
                    MarcRecord recordJSON = RecordObjectMapper.contentToMarcRecord(data);
                    ContentDTO contentDTO = RecordDTOMapper.contentToDTO(recordJSON);
                    outputStream.write(jsonbContext.marshall(contentDTO).getBytes(params.getOutputEncoding()));
                    outputStream.write("\n".getBytes());
                    break;
                case LINE:
                    MarcRecord recordLine = RecordObjectMapper.contentToMarcRecord(data);
                    outputStream.write(lineFormatWriter.write(recordLine, Charset.forName(params.getOutputEncoding())));
                    break;
                case MARCXHANGE:
                    outputStream.write(data);
                    outputStream.write("\n".getBytes());
                    break;
            }
        }
    }

}
