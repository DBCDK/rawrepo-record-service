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
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import dk.dbc.rawrepo.pool.ObjectPool;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class MarcRecordBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    @Mock
    private RelationHintsOpenAgency relationHintsOpenAgency;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    private final String COMMON = "common";
    private final String ARTICLE = "article";
    private final String AUTHORITY = "authority";
    private final String LITTOLK = "littolk";

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        doCallRealMethod().when(relationHintsOpenAgency).getAgencyPriority(anyInt());
        doCallRealMethod().when(relationHintsOpenAgency).usesCommonSchoolAgency(anyInt());
        doCallRealMethod().when(relationHintsOpenAgency).get(anyInt());
        when(relationHintsOpenAgency.usesCommonAgency(anyInt())).thenReturn(false);
        when(relationHintsOpenAgency.usesCommonAgency(eq(191919))).thenReturn(true);
        when(relationHintsOpenAgency.usesCommonAgency(eq(700300))).thenReturn(true);
        when(relationHintsOpenAgency.usesCommonAgency(eq(870970))).thenReturn(true);
        when(relationHintsOpenAgency.usesCommonAgency(eq(870971))).thenReturn(true);
        when(relationHintsOpenAgency.usesCommonAgency(eq(870974))).thenReturn(true);
        when(relationHintsOpenAgency.usesCommonAgency(eq(870979))).thenReturn(true);
    }

    private class MarcRecordBeanMock extends MarcRecordBean {
        MarcRecordBeanMock(DataSource globalDataSource) {
            super(globalDataSource);

            this.relationHints = relationHintsOpenAgency;
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

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
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

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
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(false))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, false, false), is(marcRecord));
    }

    @Test
    public void testGetMergedRecordExcludeDBCFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");


        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(false))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, true, false), is(loadMarcRecord("merged-ex-dbc-fields.xml")));
    }

    @Test
    public void testGetMergedRecordOverwriteCommonFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged-overwrite-common.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(false))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, false, true), is(marcRecord));
    }

    @Test
    public void testGetExpandedRecordOk() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, false, false, true), is(marcRecord));
    }

    @Test
    public void testGetExpandedRecordExcludeDBCFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");


        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(false))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, true, false, false), is(loadMarcRecord("merged-ex-dbc-fields.xml")));
    }

    @Test
    public void testGetExpandedRecordOverwriteCommonFields() throws Exception {
        MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged-overwrite-common.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.agencyFor(eq(bibliographicRecordId), eq(agencyId), eq(true))).thenReturn(agencyId);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

        Assert.assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, false, true, true), is(marcRecord));
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
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        Map<String, Record> recordMap = new HashMap<>();
        recordMap.put(bibliographicRecordId, record);

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.recordExists(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(eq(bibliographicRecordId), eq(agencyId))).thenReturn(true);
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
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 870970));
        final String bibliographicRecordId = "00199087";

        final Record deletedEnrichmentMock = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichment, StandardCharsets.UTF_8));
        deletedEnrichmentMock.setDeleted(true);
        deletedEnrichmentMock.setCreated(getInstant("2016-01-01"));
        deletedEnrichmentMock.setModified(getInstant("2017-01-01"));

        final Record deletedCommonMock = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommon, StandardCharsets.UTF_8));
        deletedCommonMock.setDeleted(true);

        deletedCommonMock.setCreated(getInstant("2016-01-01"));
        deletedCommonMock.setModified(getInstant("2017-01-01"));

        final Record expected = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedMerged, StandardCharsets.UTF_8));
        expected.setDeleted(true);
        expected.setCreated(getInstant("2016-01-01"));
        expected.setModified(getInstant("2017-01-01"));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentMock);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonMock);

        final Record mergedDeletedRecord = bean.fetchMergedRecord(bibliographicRecordId, 191919, getMerger());

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
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        final MarcRecord mergedMarcRecord = reader.read();
        final byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);
        Assert.assertThat(mergedContent, is(expected.getContent()));
    }

    @Test
    public void testMergeDeletedRecordExpanded() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final MarcRecord deletedEnrichmentMarcRecord = loadMarcRecord("merged-deleted/common-enrichment.xml");
        final MarcRecord deletedCommonMarcRecord = loadMarcRecord("merged-deleted/common-dbc.xml");
        final MarcRecord expectedMergedMarcRecord = loadMarcRecord("merged-deleted/expected-merged.xml");
        final MarcRecord expectedExpandedMarcRecord = loadMarcRecord("merged-deleted/expected-expanded.xml");
        final MarcRecord authorityMarcRecord = loadMarcRecord("merged-deleted/aut-dbc.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 700300, 870970));
        final String bibliographicRecordId = "50938409";

        final Record deletedEnrichmentRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichmentMarcRecord, StandardCharsets.UTF_8));
        deletedEnrichmentRecord.setDeleted(true);
        deletedEnrichmentRecord.setCreated(getInstant("2018-09-11"));
        deletedEnrichmentRecord.setModified(getInstant("2019-09-11"));

        final Record deletedCommonRecord = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommonMarcRecord, StandardCharsets.UTF_8));
        deletedCommonRecord.setDeleted(true);
        deletedCommonRecord.setCreated(getInstant("2018-09-11"));
        deletedCommonRecord.setModified(getInstant("2019-09-11"));

        final Record expectedMergedRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(expectedMergedMarcRecord, StandardCharsets.UTF_8));
        expectedMergedRecord.setDeleted(true);
        expectedMergedRecord.setCreated(getInstant("2018-09-11"));
        expectedMergedRecord.setModified(getInstant("2019-09-11"));

        final Record expectedExpandedRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(expectedExpandedMarcRecord, StandardCharsets.UTF_8));
        expectedExpandedRecord.setDeleted(true);
        expectedExpandedRecord.setCreated(getInstant("2018-09-11"));
        expectedExpandedRecord.setModified(getInstant("2019-09-11"));

        final Record authorityRecord = createRecordMock("69208045", 870979, MarcXChangeMimeType.AUTHORITY,
                marcXchangeV1Writer.write(authorityMarcRecord, StandardCharsets.UTF_8));
        authorityRecord.setDeleted(false);
        authorityRecord.setCreated(getInstant("2018-09-11"));
        authorityRecord.setModified(getInstant("2019-09-11"));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);
        when(rawRepoDAO.fetchRecord("69208045", 870979)).thenReturn(authorityRecord);

        final Record actualRecord = bean.fetchMergedRecord(bibliographicRecordId, 191919, getMerger());

        Assert.assertThat(actualRecord.getId(), is(expectedMergedRecord.getId()));
        Assert.assertThat(actualRecord.isDeleted(), is(expectedMergedRecord.isDeleted()));
        Assert.assertThat(actualRecord.getMimeType(), is(expectedMergedRecord.getMimeType()));
        Assert.assertThat(actualRecord.getCreated(), is(expectedMergedRecord.getCreated()));
        Assert.assertThat(actualRecord.getModified(), is(expectedMergedRecord.getModified()));
        Assert.assertThat(actualRecord.getTrackingId(), is(expectedMergedRecord.getTrackingId()));
        Assert.assertThat(actualRecord.getEnrichmentTrail(), is("870970,191919"));

        // MarcXchange Reader and Writer does stuff to the XML namespace and structure, so in order to do a proper
        // comparison we have to run the out content through a reader and writer first.
        InputStream inputStream = new ByteArrayInputStream(actualRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord mergedMarcRecord = reader.read();
        byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        Assert.assertThat(mergedContent, is(expectedMergedRecord.getContent()));

        bean.expandRecord(actualRecord, false);

        Assert.assertThat(actualRecord.getId(), is(expectedExpandedRecord.getId()));
        Assert.assertThat(actualRecord.isDeleted(), is(expectedExpandedRecord.isDeleted()));
        Assert.assertThat(actualRecord.getMimeType(), is(expectedExpandedRecord.getMimeType()));
        Assert.assertThat(actualRecord.getCreated(), is(expectedExpandedRecord.getCreated()));
        // We won't compare modified date as it is set during expandRecord. Therefore we can't compare to a static value
        Assert.assertThat(actualRecord.getTrackingId(), is(expectedExpandedRecord.getTrackingId()));
        Assert.assertThat(actualRecord.getEnrichmentTrail(), is("870970,191919"));

        inputStream = new ByteArrayInputStream(actualRecord.getContent());
        bufferedInputStream = new BufferedInputStream(inputStream);
        reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        mergedMarcRecord = reader.read();
        mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);
        Assert.assertThat(mergedContent, is(expectedExpandedRecord.getContent()));
    }

    @Test
    public void testFetchRecordCollectionExistingRecord() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "90004158";
        final int agencyId = 191919;

        Map<String, Record> collection = new HashMap<>();

        final MarcRecord marcRecord = loadMarcRecord("merged-deleted/expected-merged.xml");

        final Record record = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));
        record.setDeleted(true);
        record.setCreated(getInstant("2018-09-11"));
        record.setModified(getInstant("2019-09-11"));

        collection.put(bibliographicRecordId, record);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.fetchRecordCollection(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class))).thenReturn(collection);

        Assert.assertThat(bean.fetchRecordCollection(bibliographicRecordId, agencyId, getMerger()), is(collection));
    }

    @Test
    public void testFetchRecordCollectionDeletedRecord() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final MarcRecord deletedEnrichmentMarcRecord = loadMarcRecord("merged-deleted/common-enrichment.xml");
        final MarcRecord deletedCommonMarcRecord = loadMarcRecord("merged-deleted/common-dbc.xml");
        final MarcRecord expectedMergedMarcRecord = loadMarcRecord("merged-deleted/expected-merged.xml");
        final MarcRecord authorityMarcRecord = loadMarcRecord("merged-deleted/aut-dbc.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 700300, 870970));
        final String bibliographicRecordId = "50938409";
        final String autBibliographicRecordId = "69208045";

        final Record deletedEnrichmentRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichmentMarcRecord, StandardCharsets.UTF_8));
        deletedEnrichmentRecord.setDeleted(true);
        deletedEnrichmentRecord.setCreated(getInstant("2018-09-11"));
        deletedEnrichmentRecord.setModified(getInstant("2019-09-11"));

        final Record deletedCommonRecord = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommonMarcRecord, StandardCharsets.UTF_8));
        deletedCommonRecord.setDeleted(true);
        deletedCommonRecord.setCreated(getInstant("2018-09-11"));
        deletedCommonRecord.setModified(getInstant("2019-09-11"));

        final Record expectedMergedRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(expectedMergedMarcRecord, StandardCharsets.UTF_8));
        expectedMergedRecord.setDeleted(true);
        expectedMergedRecord.setCreated(getInstant("2018-09-11"));
        expectedMergedRecord.setModified(getInstant("2019-09-11"));

        final Record authorityRecord = createRecordMock("69208045", 870979, MarcXChangeMimeType.AUTHORITY,
                marcXchangeV1Writer.write(authorityMarcRecord, StandardCharsets.UTF_8));
        authorityRecord.setDeleted(false);
        authorityRecord.setCreated(getInstant("2018-09-11"));
        authorityRecord.setModified(getInstant("2019-09-11"));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(autBibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(autBibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(autBibliographicRecordId, 870979)).thenReturn(true);
        when(rawRepoDAO.recordExists(autBibliographicRecordId, 870979)).thenReturn(true);
        when(rawRepoDAO.findParentRelationAgency(autBibliographicRecordId, 191919)).thenReturn(870979);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(rawRepoDAO.agencyFor("69208045", 191919, true)).thenReturn(191919);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);
        when(rawRepoDAO.fetchMergedRecord(eq(autBibliographicRecordId), eq(191919), any(MarcXMerger.class), eq(false))).thenReturn(authorityRecord);

        final Map<String, Record> actual = bean.fetchRecordCollection(bibliographicRecordId, 191919, getMerger());

        Assert.assertThat(actual.size(), is(2));

        Assert.assertTrue(actual.containsKey(bibliographicRecordId));
        Assert.assertTrue(actual.containsKey(autBibliographicRecordId));

        final Record actualCommonRecord = actual.get(bibliographicRecordId);

        Assert.assertThat(actualCommonRecord.getId(), is(expectedMergedRecord.getId()));
        Assert.assertThat(actualCommonRecord.isDeleted(), is(expectedMergedRecord.isDeleted()));
        Assert.assertThat(actualCommonRecord.getMimeType(), is(expectedMergedRecord.getMimeType()));
        Assert.assertThat(actualCommonRecord.getCreated(), is(expectedMergedRecord.getCreated()));
        Assert.assertThat(actualCommonRecord.getModified(), is(expectedMergedRecord.getModified()));
        Assert.assertThat(actualCommonRecord.getTrackingId(), is(expectedMergedRecord.getTrackingId()));

        InputStream inputStream = new ByteArrayInputStream(actualCommonRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord mergedMarcRecord = reader.read();
        byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        Assert.assertThat(mergedContent, is(expectedMergedRecord.getContent()));

        final Record actualAutRecord = actual.get(autBibliographicRecordId);

        Assert.assertThat(actualAutRecord.getId(), is(authorityRecord.getId()));
        Assert.assertThat(actualAutRecord.isDeleted(), is(authorityRecord.isDeleted()));
        Assert.assertThat(actualAutRecord.getMimeType(), is(authorityRecord.getMimeType()));
        Assert.assertThat(actualAutRecord.getCreated(), is(authorityRecord.getCreated()));
        Assert.assertThat(actualAutRecord.getModified(), is(authorityRecord.getModified()));
        Assert.assertThat(actualAutRecord.getTrackingId(), is(authorityRecord.getTrackingId()));

        inputStream = new ByteArrayInputStream(actualAutRecord.getContent());
        bufferedInputStream = new BufferedInputStream(inputStream);
        reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        mergedMarcRecord = reader.read();
        mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        Assert.assertThat(mergedContent, is(authorityRecord.getContent()));
    }

    @Test
    public void testFetchRecordCollectionExpandedExistingRecord() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "90004158";
        final int agencyId = 191919;
        final ObjectPool<MarcXMerger> mergePool = new DefaultMarcXMergerPool();
        final MarcXMerger merger = mergePool.checkOut();
        final Map<String, Record> collection = new HashMap<>();
        final MarcRecord marcRecord = loadMarcRecord("merged-deleted/expected-expanded.xml");

        final Record record = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));
        record.setDeleted(true);
        record.setCreated(getInstant("2018-09-11"));
        record.setModified(getInstant("2019-09-11"));

        collection.put(bibliographicRecordId, record);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, merger, true, false)).thenReturn(collection);

        Assert.assertThat(bean.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, merger, true, false), is(collection));
    }

    @Test
    public void testFetchRecordCollectionExpandedDeletedRecord() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final MarcRecord deletedEnrichmentMarcRecord = loadMarcRecord("merged-deleted/common-enrichment.xml");
        final MarcRecord deletedCommonMarcRecord = loadMarcRecord("merged-deleted/common-dbc.xml");
        final MarcRecord expanded = loadMarcRecord("merged-deleted/expected-expanded.xml");
        final MarcRecord authorityMarcRecord = loadMarcRecord("merged-deleted/aut-dbc.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 700300, 870970));
        final String bibliographicRecordId = "50938409";
        final String autBibliographicRecordId = "69208045";

        final Record deletedEnrichmentRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichmentMarcRecord, StandardCharsets.UTF_8));
        deletedEnrichmentRecord.setDeleted(true);
        deletedEnrichmentRecord.setCreated(getInstant("2018-09-11"));
        deletedEnrichmentRecord.setModified(getInstant("2019-09-11"));

        final Record deletedCommonRecord = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommonMarcRecord, StandardCharsets.UTF_8));
        deletedCommonRecord.setDeleted(true);
        deletedCommonRecord.setCreated(getInstant("2018-09-11"));
        deletedCommonRecord.setModified(getInstant("2019-09-11"));

        final Record expectedExpandedRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(expanded, StandardCharsets.UTF_8));
        expectedExpandedRecord.setDeleted(true);
        expectedExpandedRecord.setCreated(getInstant("2018-09-11"));
        expectedExpandedRecord.setModified(getInstant("2019-09-11"));

        final Record authorityRecord = createRecordMock("69208045", 870979, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(authorityMarcRecord, StandardCharsets.UTF_8));
        authorityRecord.setDeleted(false);
        authorityRecord.setCreated(getInstant("2018-09-11"));
        authorityRecord.setModified(getInstant("2019-09-11"));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(autBibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(autBibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(autBibliographicRecordId, 870979)).thenReturn(true);
        when(rawRepoDAO.recordExists(autBibliographicRecordId, 870979)).thenReturn(true);
        when(rawRepoDAO.findParentRelationAgency(autBibliographicRecordId, 191919)).thenReturn(870979);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);
        when(rawRepoDAO.fetchMergedRecordExpanded(eq(autBibliographicRecordId), eq(191919), any(MarcXMerger.class), eq(false))).thenReturn(authorityRecord);
        when(rawRepoDAO.fetchRecord(autBibliographicRecordId, 870979)).thenReturn(authorityRecord);

        final Map<String, Record> actual = bean.fetchRecordCollectionExpanded(bibliographicRecordId, 191919, getMerger(), true, false);

        Assert.assertThat(actual.size(), is(2));
        Assert.assertTrue(actual.containsKey(bibliographicRecordId));
        Assert.assertTrue(actual.containsKey(autBibliographicRecordId));

        final Record actualCommonRecord = actual.get(bibliographicRecordId);

        Assert.assertThat(actualCommonRecord.getId(), is(expectedExpandedRecord.getId()));
        Assert.assertThat(actualCommonRecord.isDeleted(), is(expectedExpandedRecord.isDeleted()));
        Assert.assertThat(actualCommonRecord.getMimeType(), is(expectedExpandedRecord.getMimeType()));
        Assert.assertThat(actualCommonRecord.getCreated(), is(expectedExpandedRecord.getCreated()));
        // We won't compare modified date as it is set during expandRecord. Therefore we can't compare to a static value
        Assert.assertThat(actualCommonRecord.getTrackingId(), is(expectedExpandedRecord.getTrackingId()));

        InputStream inputStream = new ByteArrayInputStream(actualCommonRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord mergedMarcRecord = reader.read();
        byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        Assert.assertThat(mergedContent, is(expectedExpandedRecord.getContent()));

        final Record actualAutRecord = actual.get(autBibliographicRecordId);

        Assert.assertThat(actualAutRecord.getId(), is(authorityRecord.getId()));
        Assert.assertThat(actualAutRecord.isDeleted(), is(authorityRecord.isDeleted()));
        Assert.assertThat(actualAutRecord.getMimeType(), is(authorityRecord.getMimeType()));
        Assert.assertThat(actualAutRecord.getCreated(), is(authorityRecord.getCreated()));
        Assert.assertThat(actualAutRecord.getModified(), is(authorityRecord.getModified()));
        Assert.assertThat(actualAutRecord.getTrackingId(), is(authorityRecord.getTrackingId()));

        inputStream = new ByteArrayInputStream(actualAutRecord.getContent());
        bufferedInputStream = new BufferedInputStream(inputStream);
        reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        mergedMarcRecord = reader.read();
        mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        Assert.assertThat(mergedContent, is(authorityRecord.getContent()));
    }

    @Test
    public void recordIsActiveTestActive() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(true);

        Assert.assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(true));
    }

    @Test
    public void recordIsActiveTestDeleted() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(false);

        Assert.assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(false));
    }

    @Test(expected = RecordNotFoundException.class)
    public void recordIsActiveTestNotFound() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(false);

        Assert.assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(true));
    }

    @Test
    public void findParentRelationAgencyTestActive() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(rawRepoDAO.recordExistsMaybeDeleted(anyString(), eq(191919))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(ARTICLE, 870971)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(AUTHORITY, 870979)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(LITTOLK, 870974)).thenReturn(true);

        when(rawRepoDAO.recordExists(anyString(), eq(191919))).thenReturn(true);
        when(rawRepoDAO.recordExists(COMMON, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(ARTICLE, 870971)).thenReturn(true);
        when(rawRepoDAO.recordExists(LITTOLK, 870974)).thenReturn(true);
        when(rawRepoDAO.recordExists(AUTHORITY, 870979)).thenReturn(true);

        assertFindParentRelationAgency(bean);
    }

    @Test
    public void findParentRelationAgencyTestNotActive() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(rawRepoDAO.recordExistsMaybeDeleted(anyString(), eq(191919))).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(ARTICLE, 870971)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(AUTHORITY, 870979)).thenReturn(true);
        when(rawRepoDAO.recordExistsMaybeDeleted(LITTOLK, 870974)).thenReturn(true);

        when(rawRepoDAO.recordExists(anyString(), eq(191919))).thenReturn(false);
        when(rawRepoDAO.recordExists(COMMON, 870970)).thenReturn(false);
        when(rawRepoDAO.recordExists(ARTICLE, 870971)).thenReturn(false);
        when(rawRepoDAO.recordExists(LITTOLK, 870974)).thenReturn(false);
        when(rawRepoDAO.recordExists(AUTHORITY, 870979)).thenReturn(false);

        assertFindParentRelationAgency(bean);
    }

    private void assertFindParentRelationAgency(MarcRecordBean bean) throws Exception {
        Assert.assertThat(bean.findParentRelationAgency(COMMON, 191919), is(870970));
        Assert.assertThat(bean.findParentRelationAgency(COMMON, 870970), is(870970));

        Assert.assertThat(bean.findParentRelationAgency(ARTICLE, 191919), is(870971));
        Assert.assertThat(bean.findParentRelationAgency(ARTICLE, 870971), is(870971));

        Assert.assertThat(bean.findParentRelationAgency(LITTOLK, 191919), is(870974));
        Assert.assertThat(bean.findParentRelationAgency(LITTOLK, 870974), is(870974));

        Assert.assertThat(bean.findParentRelationAgency(AUTHORITY, 191919), is(870979));
        Assert.assertThat(bean.findParentRelationAgency(AUTHORITY, 870979), is(870979));
    }

    @Test
    public void findParentRelationAgencyFBSLocal() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(rawRepoDAO.recordExistsMaybeDeleted(anyString(), eq(820010))).thenReturn(true);
        when(rawRepoDAO.recordExists(anyString(), eq(820010))).thenReturn(false);

        Assert.assertThat(bean.findParentRelationAgency("FBS", 820010), is(820010));
    }

    @Test
    public void testGetRelationsParents191919() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(COMMON, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(COMMON, 870970)).thenReturn(false);

        Set<RecordId> actual = bean.getRelationsParents(COMMON, 191919);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsParentsFBSEnrichment() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "50938409";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/fbs-enrichment.xml");

        final Record record = createRecordMock(bibliographicRecordId, 911116, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 911116)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 911116)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 911116)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 911116);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsParentsFBSLocal() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "2207787";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/fbs-local.xml");

        final Record record = createRecordMock(bibliographicRecordId, 820010, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 820010)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 820010)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 820010)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 820010);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("223906", 820010)));
    }

    @Test
    public void testGetRelationsParentsCommonNoParents() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "50938409";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-no-relations.xml");

        final Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsParentsCommonSingleAut() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "50938409";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-single-aut.xml");

        final Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("69208045", 870979)));
    }

    @Test
    public void testGetRelationsParentsCommonTripleAut() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "53090567";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-triple-aut.xml");

        Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(4));

        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("19050416", 870979)));
        Assert.assertThat(iterator.next(), is(new RecordId("19050785", 870979)));
        Assert.assertThat(iterator.next(), is(new RecordId("19047903", 870979)));
        Assert.assertThat(iterator.next(), is(new RecordId("69208045", 870979)));
    }

    @Test
    public void testGetRelationsParentsArticle() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "85803190";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/article.xml");

        Record record = createRecordMock(bibliographicRecordId, 870971, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870971)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870971)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870971)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870971);

        Assert.assertThat(actual.size(), is(1));

        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("23191083", 870970)));
    }

    @Test
    public void testGetRelationsParentsVolume() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "22723715";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-volume.xml");

        Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(1));

        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("50434990", 870970)));
    }

    @Test
    public void testGetRelationsParentsLittolk() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "126395604";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/littolk.xml");

        Record record = createRecordMock(bibliographicRecordId, 870974, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870974)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870974)).thenReturn(false);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870974)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870974);

        Assert.assertThat(actual.size(), is(3));

        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId("68754011", 870979)));
        Assert.assertThat(iterator.next(), is(new RecordId("68234190", 870979)));
        Assert.assertThat(iterator.next(), is(new RecordId("46912683", 870970)));
    }

    @Test
    public void testGetRelationsSiblingsFromMeActiveRecord191919() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 191919);
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsFromMe(eq(thisRecordId))).thenReturn(new HashSet<>(Collections.singletonList(siblingFromMeRecordId)));

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeActiveRecord870970() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 870970);


        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsFromMe(eq(thisRecordId))).thenReturn(new HashSet<>());

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive191919() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive700300() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 700300)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 700300)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 700300);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive870970() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeActiveRecord191919() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 191919);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsToMe(eq(thisRecordId))).thenReturn(new HashSet<>());

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 191919);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeActiveRecord870970() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 870970);

        final RecordId siblingToMeRecordId = new RecordId(bibliographicRecordId, 191919);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsToMe(eq(thisRecordId))).thenReturn(new HashSet<>(Collections.singletonList(siblingToMeRecordId)));

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(siblingToMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive191919() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 191919);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive700300() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 700300)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 700300)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 700300);

        Assert.assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive870970() throws Exception {
        final MarcRecordBean bean = new MarcRecordBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 870970);

        Assert.assertThat(actual.size(), is(2));
        Iterator<RecordId> iterator = actual.iterator();
        Assert.assertThat(iterator.next(), is(new RecordId(bibliographicRecordId, 191919)));
        Assert.assertThat(iterator.next(), is(new RecordId(bibliographicRecordId, 700300)));
    }

    private MarcRecord loadMarcRecord(String filename) throws Exception {
        InputStream inputStream = this.getClass().getResourceAsStream(filename);
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

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
