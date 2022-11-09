package dk.dbc.marc;

import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Pattern;

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
public class MarcFormatDeducer {
    private static final Pattern VALID_DANMARC2_LINE_FORMAT = Pattern.compile(
            "^\\p{Digit}{3}\\s+?(\\p{IsLatin}|[0-9 ]{2})?\\s?\\*",
            Pattern.MULTILINE);

    private static final Pattern VALID_LINE_FORMAT = Pattern.compile(
            "^\\p{Digit}{3}\\s+?(\\p{IsLatin}|[0-9 ]{2})?\\s?\\$",
            Pattern.MULTILINE);

    public enum FORMAT {
        DANMARC2_LINE,
        JSONL,
        LINE,
        MARCXCHANGE,
        MARCXML,
        ISO2709
    }

    private final int prologSize;

    /**
     * @param prologSize number of leading bytes to read when
     *                   attempting to deduce MARC format
     */
    public MarcFormatDeducer(int prologSize) {
        this.prologSize = prologSize;
    }

    /**
     * Deduces the MARC format by looking at a sample of the
     * given input stream
     * @param is input stream to examine
     * @param encoding input data encoding
     * @return deduced format (ISO2709 is the fallback)
     */
    public FORMAT deduce(PushbackInputStream is, Charset encoding) {
        final byte[] buffer = new byte[prologSize];
        try {
            final int bytesRead = blockingRead(is,buffer);
            if (bytesRead > 0) {
                final String prolog = safeToString(buffer, encoding);
                try {
                    if (isJson(prolog)) {
                        return FORMAT.JSONL;
                    } else if (isMarcxchange(prolog)) {
                        return FORMAT.MARCXCHANGE;
                    } else if (isMarcXml(prolog)) {
                        return FORMAT.MARCXML;
                    } else if (isLineFormat(prolog)) {
                        return FORMAT.LINE;
                    } else if (isDanmarc2LineFormat(prolog)) {
                        return FORMAT.DANMARC2_LINE;
                    }
                } finally {
                    is.unread(buffer, 0, bytesRead);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("Error while trying to deduce MARC format", e);
        }
        return FORMAT.ISO2709;
    }

    private String safeToString(byte[] bytes, Charset encoding) {
        return safeToString(bytes, encoding, 8);
    }

    private String safeToString(byte[] bytes, Charset encoding, int retries) {
        // Since the last of the prolog bytes might be in the
        // middle of a multi-byte character which might cause
        // the character set decoding to throw an exception
        // we do a number of retries.
        if (retries > 0) {
            try {
                return new String(bytes, encoding);
            } catch (RuntimeException e) {
                if (bytes.length > 5) {
                    return safeToString(
                            Arrays.copyOfRange(bytes, 0, bytes.length - 1),
                            encoding, retries - 1);
                }
            }
        }
        return "";
    }

    private boolean isJson(String string) {
        return string.startsWith("{") || string.startsWith("[");
    }

    private boolean isMarcxchange(String string) {
        return string.startsWith("<") && string.contains("info:lc/xmlns/marcxchange-v1");
    }

    private boolean isMarcXml(String string) {
        return string.startsWith("<") && string.contains("http://www.loc.gov/MARC21/slim");
    }

    private boolean isDanmarc2LineFormat(String string) {
        return VALID_DANMARC2_LINE_FORMAT.matcher(string).find();
    }

    private boolean isLineFormat(String string) {
        return VALID_LINE_FORMAT.matcher(string).find();
    }

    static int blockingRead(PushbackInputStream is, byte[] buffer) throws IOException {
        int expected = buffer.length;
        int alreadyRead = is.read(buffer);
        if (alreadyRead == -1) { return -1; }

        while (alreadyRead < expected) {
            int missing = expected - alreadyRead;
            int read_result = is.read(buffer, alreadyRead, missing);
            if (read_result == -1) return alreadyRead;
            alreadyRead+=read_result;
        }

        return alreadyRead;
    }

}
