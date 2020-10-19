/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.output;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.rawrepo.dto.ContentDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;

import java.io.IOException;
import java.io.OutputStream;

public class OutputStreamJsonRecordWriter implements OutputStreamRecordWriter {
    private final OutputStream outputStream;
    private final String encoding;
    private final JSONBContext jsonbContext = new JSONBContext();

    public OutputStreamJsonRecordWriter(OutputStream stream, String encoding) {
        this.outputStream = stream;
        this.encoding = encoding;
    }

    @Override
    public void write(MarcRecord marcRecord) throws JSONBException, IOException {
        ContentDTO contentDTO = RecordDTOMapper.contentToDTO(marcRecord);
        synchronized (this) {
            outputStream.write(jsonbContext.marshall(contentDTO).getBytes(encoding));
            outputStream.write("\n".getBytes());
        }
    }
}
