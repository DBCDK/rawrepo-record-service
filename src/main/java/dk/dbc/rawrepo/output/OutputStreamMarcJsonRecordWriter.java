package dk.dbc.rawrepo.output;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.JsonWriter;
import dk.dbc.marc.writer.MarcWriterException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class OutputStreamMarcJsonRecordWriter implements OutputStreamRecordWriter {
    private final OutputStream outputStream;
    private final String encoding;
    private final JsonWriter jsonWriter = new JsonWriter();

    public OutputStreamMarcJsonRecordWriter(OutputStream stream, String encoding) {
        this.outputStream = stream;
        this.encoding = encoding;
    }

    @Override
    public void write(MarcRecord marcRecord) throws MarcWriterException, IOException {
        synchronized (this) {
            outputStream.write(jsonWriter.write(marcRecord, Charset.forName(encoding)));
            outputStream.write("\n".getBytes(encoding));
        }
    }
}
