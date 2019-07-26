/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class MarcRecordBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    private class MarcRecordBeanMock extends MarcRecordBean {
        public MarcRecordBeanMock(DataSource globalDataSource) {
            super(globalDataSource);
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
            return rawRepoDAO;
        }
    }

    @Test(expected = RecordNotFoundException.class)
    public void testGetRawRecordNotFound() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "123456789";
        int agencyId = 191919;

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.MARCXCHANGE, null);

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.fetchRecord(eq(bibliographicRecordId), eq(agencyId))).thenReturn(record);

        bean.getMarcRecord(bibliographicRecordId, agencyId, true, false);
    }

    @Test
    public void testGetRawRecordOk() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("raw.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.fetchRecord(eq(bibliographicRecordId), eq(agencyId))).thenReturn(record);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);

        Assert.assertThat(bean.getMarcRecord(bibliographicRecordId, agencyId, true, false), is(marcRecord));
    }

    @Test
    public void testGetMergedRecordOk() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, false, false), is(marcRecord));
    }

    @Test
    public void testGetMergedRecordExcludeDBCFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");


        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, true, false), is(loadMarcRecord("merged-ex-dbc-fields.xml")));
    }

    @Test
    public void testGetMergedRecordOverwriteCommonFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged-overwrite-common.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, false, true), is(marcRecord));
    }

    @Test
    public void testGetExpandedRecordOk() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, false, false, false), is(marcRecord));
    }

    @Test
    public void testGetExpandedRecordExcludeDBCFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");


        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, true, false, false), is(loadMarcRecord("merged-ex-dbc-fields.xml")));
    }

    @Test
    public void testGetExpandedRecordOverwriteCommonFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged-overwrite-common.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, false, true, false), is(marcRecord));
    }

    @Test
    public void testGetCollectionRecordOverwriteCommonFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged-overwrite-common.xml");

        Collection<MarcRecord> collection = new HashSet<>();
        collection.add(marcRecord);

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, Charset.forName("UTF-8")));

        Map<String, Record> recordMap = new HashMap<>();
        recordMap.put(bibliographicRecordId, record);

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.fetchRecordCollectionExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true), eq(false))).thenReturn(recordMap);

        Assert.assertThat(bean.getMarcRecordCollection(bibliographicRecordId, agencyId, true, false, true, true, false), is(collection));
    }

    @Test
    public void testGetAllAgenciesForBibliographicRecordId() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        String bibliographicRecordId = "123456789";

        Set<Integer> agencySet = new HashSet<>(Arrays.asList(191919, 870970));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agencySet);

        Assert.assertThat(bean.getAllAgenciesForBibliographicRecordId(bibliographicRecordId), is(agencySet));
    }

    @Test
    public void testParentCommonAgencyId1() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870970));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        Assert.assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870970));
    }

    @Test
    public void testParentCommonAgencyId2() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        String bibliographicRecordId = "12345678";
        Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870971));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        Assert.assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870971));
    }

    @Test
    public void testParentCommonAgencyId3() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870979));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        Assert.assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870979));
    }

    @Test(expected = RecordNotFoundException.class)
    public void testParentCommonAgencyId4() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO);
    }

    @Test
    public void testMergeDeletedRecord() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final MarcRecord deletedEnrichment = loadMarcRecord("deleted-191919.xml");
        final MarcRecord deletedCommon = loadMarcRecord("deleted-870970.xml");
        final MarcRecord deletedMerged = loadMarcRecord("deleted-merged.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(870970));
        final String bibliographicRecordId = "00199087";

        final Record deletedEnrichmentMock = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichment, Charset.forName("UTF-8")));
        deletedEnrichmentMock.setDeleted(true);
        deletedEnrichmentMock.setCreated(getInstant("2016-01-01"));
        deletedEnrichmentMock.setModified(getInstant("2017-01-01"));

        final Record deletedCommonMock = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommon, Charset.forName("UTF-8")));
        deletedCommonMock.setDeleted(true);

        deletedCommonMock.setCreated(getInstant("2016-01-01"));
        deletedCommonMock.setModified(getInstant("2017-01-01"));

        final Record expected = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedMerged, Charset.forName("UTF-8")));
        expected.setDeleted(true);
        expected.setCreated(getInstant("2016-01-01"));
        expected.setModified(getInstant("2017-01-01"));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentMock);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonMock);

        final Record mergedDeletedRecord = bean.mergeDeletedRecord(bibliographicRecordId, 191919, getMerger(), rawRepoDAO);

        Assert.assertThat(mergedDeletedRecord.getId(), is(expected.getId()));
        Assert.assertThat(mergedDeletedRecord.isDeleted(), is(expected.isDeleted()));
        Assert.assertThat(mergedDeletedRecord.getMimeType(), is(expected.getMimeType()));
        Assert.assertThat(mergedDeletedRecord.getCreated(), is(expected.getCreated()));
        Assert.assertThat(mergedDeletedRecord.getModified(), is(expected.getModified()));
        Assert.assertThat(mergedDeletedRecord.getTrackingId(), is(expected.getTrackingId()));
        Assert.assertThat(mergedDeletedRecord.getEnrichmentTrail(), is("870970,191919"));

        // MarcXchange Reader and Writer does stuff to the XML namespace and structure, so in order to do a proper
        // comparison we have to run the out content through a reader and writer first.
        final InputStream inputStream = new ByteArrayInputStream(mergedDeletedRecord.getContent());
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, Charset.forName("UTF-8"));
        final MarcRecord mergedMarcRecord = reader.read();
        final byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, Charset.forName("UTF-8"));
        Assert.assertThat(mergedContent, is(expected.getContent()));
    }

    private MarcRecord loadMarcRecord(String filename) throws Exception {
        InputStream inputStream = this.getClass().getResourceAsStream(filename);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, Charset.forName("UTF-8"));

        return reader.read();
    }

    private Record createRecordMock(String bibliographicRecordId, int agencyId, String mimetype, byte[] content) {
        Record mock = new RawRepoRecordMock(bibliographicRecordId, agencyId);
        mock.setMimeType(mimetype);
        mock.setDeleted(false);
        mock.setContent(content);

        return mock;
    }

    private Instant getInstant(String s) {
        LocalDate localDate = LocalDate.parse(s);
        LocalDateTime localDateTime = localDate.atStartOfDay();
        return localDateTime.toInstant(ZoneOffset.UTC);
    }

    private MarcXMerger getMerger() throws Exception {
        final String immutable = "001;010;020;990;991;996";
        final String overwrite = "004;005;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

        final FieldRules customFieldRules = new FieldRules(immutable, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);

        return new MarcXMerger(customFieldRules, "CUSTOM");
    }
}
