package dk.dbc.rawrepo.output;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.MarcWriterException;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

public class OutputStreamLineRecordWriter implements OutputStreamRecordWriter {
    private OutputStream outputStream;
    private String encoding;
    private final DanMarc2LineFormatWriter lineFormatWriter = new DanMarc2LineFormatWriter();

    public OutputStreamLineRecordWriter(OutputStream stream, String encoding) {
        this.outputStream = stream;
        this.encoding = encoding;
    }

    @Override
    public void write(MarcRecord marcRecord) throws MarcWriterException, IOException {
        synchronized (this) {
            outputStream.write(lineFormatWriter.write(marcRecord, Charset.forName(encoding)));
        }
    }
}
