/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.output;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.MarcXchangeV1Writer;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class OutputStreamMarcXchangeRecordWriter implements OutputStreamRecordWriter {
    private final OutputStream outputStream;
    private final String encoding;
    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    public OutputStreamMarcXchangeRecordWriter(OutputStream stream, String encoding) {
        this.outputStream = stream;
        this.encoding = encoding;
    }

    @Override
    public void write(MarcRecord marcRecord) throws IOException {
        synchronized (this) {
            outputStream.write(marcXchangeV1Writer.write(marcRecord, Charset.forName(encoding)));
            outputStream.write("\n".getBytes());
        }
    }
}
