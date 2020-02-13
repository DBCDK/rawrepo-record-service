package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXMerger;

import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class BeanTestHelper {

    public static MarcRecord loadMarcRecord(String filename) throws Exception {
        InputStream inputStream = BeanTestHelper.class.getResourceAsStream(filename);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        return reader.read();
    }

    public static Record createRecordMock(String bibliographicRecordId, int agencyId, String mimetype, byte[] content) {
        Record mock = new RawRepoRecordMock(bibliographicRecordId, agencyId);
        mock.setMimeType(mimetype);
        mock.setDeleted(false);
        mock.setContent(content);

        return mock;
    }

    public static Instant getInstant(String s) {
        LocalDate localDate = LocalDate.parse(s);
        LocalDateTime localDateTime = localDate.atStartOfDay();
        return localDateTime.toInstant(ZoneOffset.UTC);
    }

    public static MarcXMerger getMerger() throws Exception {
        final String immutable = "001;010;020;990;991;996";
        final String overwrite = "004;005;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

        final FieldRules customFieldRules = new FieldRules(immutable, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);

        return new MarcXMerger(customFieldRules, "CUSTOM");
    }
}
