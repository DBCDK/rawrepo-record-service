package dk.dbc.marc;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.LineFormatWriter;
import dk.dbc.marc.writer.MarcWriterException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;

/**
 *
 * <pre>
 * This class is copied from the mconv project because mconv can't set as a maven dependency.
 * That problem should be fixed and this class removed.
 * The namespace is the same here as in mconv, so no other code changes should be necessary.
 * See issue <a href="https://dbcjira.atlassian.net/browse/MS-4134">MS-4134</a>
 * </pre>
 */
// TODO Remove class and instead import from dk.dbc.mconv jar
public class LineFormatConcatWriter extends LineFormatWriter {
    @Override
    public byte[] write(MarcRecord marcRecord, Charset encoding)
            throws UnsupportedCharsetException, MarcWriterException {
        final byte[] bytes = super.write(marcRecord, encoding);
        if (bytes != null) {
            final ByteArrayOutputStream buffer = new ByteArrayOutputStream();
            final String[] lines = new String(bytes, encoding).split("\n");
            try {
                for (int i = 0; i < lines.length; i++) {
                    String concatLine = "\"" + lines[i].replaceAll("\"", "\\\\\\\"") + "\\n\"";
                    if (i < lines.length - 1) {
                        concatLine += " +\n";
                    }
                    buffer.write(concatLine.getBytes(encoding));
                }
                buffer.write("\n".getBytes(encoding));
            } catch (IOException e) {
                throw new MarcWriterException("", e);
            }
            return buffer.toByteArray();
        }
        return null;
    }
}
