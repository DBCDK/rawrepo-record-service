package dk.dbc.marc;

import java.io.IOException;
import java.io.PushbackInputStream;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.regex.Pattern;

/**
 *
 * This class is copied from the mconv project. For some reason importing dk.dbc.mconv dependency breaks the project.
 * <p>As soon as mconv is imported the following exception occurs:</p>
 * <pre>
 * Caused by: javax.ws.rs.ProcessingException: A MultiException has 2 exceptions.  They are:
 * 1. java.util.ServiceConfigurationError: com.fasterxml.jackson.databind.Module: com.fasterxml.jackson.datatype.jsr310.JSR310Module not a subtype
 * 2. java.lang.IllegalStateException: Unable to perform operation: create on org.glassfish.jersey.jackson.internal.DefaultJacksonJaxbJsonProvider
 *
 * 	at dk.dbc.httpclient.HttpClient.execute(HttpClient.java:144)
 * 	at dk.dbc.httpclient.FailSafeHttpClient.lambda$execute$1(FailSafeHttpClient.java:65)
 * 	at net.jodah.failsafe.Functions.lambda$get$0(Functions.java:48)
 * 	at net.jodah.failsafe.RetryPolicyExecutor.lambda$supply$0(RetryPolicyExecutor.java:62)
 * 	at net.jodah.failsafe.Execution.executeSync(Execution.java:129)
 * 	at net.jodah.failsafe.FailsafeExecutor.call(FailsafeExecutor.java:376)
 * 	at net.jodah.failsafe.FailsafeExecutor.get(FailsafeExecutor.java:67)
 * 	at dk.dbc.httpclient.FailSafeHttpClient.execute(FailSafeHttpClient.java:65)
 * 	at dk.dbc.httpclient.HttpRequest.execute(HttpRequest.java:31)
 * 	at dk.dbc.vipcore.VipCoreConnector.postRequest(VipCoreConnector.java:123)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.postLibraryRulesRequest(VipCoreLibraryRulesConnector.java:180)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.hasFeature(VipCoreLibraryRulesConnector.java:137)
 * 	at dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector.hasFeature(VipCoreLibraryRulesConnector.java:133)
 * 	at dk.dbc.rawrepo.RelationHintsVipCore.usesCommonAgency(RelationHintsVipCore.java:28)
 * 	at dk.dbc.rawrepo.RecordRelationsBean.findParentRelationAgency(RecordRelationsBean.java:79)
 * 	at dk.dbc.rawrepo.RecordRelationsBean.parentIsActive(RecordRelationsBean.java:213)
 * 	</pre>
 *
 * 	That problem should be fixed and this class removed. The namespace is the same here as in mconv, so no other code changes should be necessary. See issue <a href="https://dbcjira.atlassian.net/browse/MS-4134">MS-4134</a>
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
