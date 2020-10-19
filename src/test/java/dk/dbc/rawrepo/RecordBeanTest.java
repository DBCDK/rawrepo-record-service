/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static dk.dbc.rawrepo.BeanTestHelper.createRecordMock;
import static dk.dbc.rawrepo.BeanTestHelper.getInstant;
import static dk.dbc.rawrepo.BeanTestHelper.loadMarcRecord;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

public class RecordBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    @Mock
    private RecordSimpleBean recordSimpleBean;

    @Mock
    private RecordRelationsBean recordRelationsBean;

    @Mock
    private static RelationHintsOpenAgency relationHintsOpenAgency;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    private class RecordBeanMock extends RecordBean {
        RecordBeanMock(DataSource globalDataSource, RecordSimpleBean recordSimpleBean, RecordRelationsBean recordRelationsBean) {
            super(globalDataSource);

            this.relationHints = relationHintsOpenAgency;
            this.recordSimpleBean = recordSimpleBean;
            this.recordRelationsBean = recordRelationsBean;
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

            return rawRepoDAO;
        }
    }

    private RecordBeanMock initRecordBeanMock() {
        return new RecordBeanMock(globalDataSource, recordSimpleBean, recordRelationsBean);
    }

    @Test
    public void testParentCommonAgencyId1() throws Exception {
        final RecordBean bean = initRecordBeanMock();
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870970));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870970));
    }

    @Test
    public void testParentCommonAgencyId2() throws Exception {
        RecordBean bean = initRecordBeanMock();
        String bibliographicRecordId = "12345678";
        Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870971));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870971));
    }

    @Test
    public void testParentCommonAgencyId3() throws Exception {
        final RecordBean bean = initRecordBeanMock();
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321, 870979));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        assertThat(bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO), is(870979));
    }

    @Test
    public void testParentCommonAgencyId4() throws Exception {
        final RecordBean bean = initRecordBeanMock();
        final String bibliographicRecordId = "12345678";
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(123456, 654321));

        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);

        Assertions.assertThrows(RecordNotFoundException.class, () -> {
            bean.parentCommonAgencyId(bibliographicRecordId, rawRepoDAO);
        });
    }

    @Test
    public void testMergeDeletedRecord() throws Exception {
        final RecordBean bean = initRecordBeanMock();

        final MarcRecord deletedEnrichment = loadMarcRecord("deleted-191919.xml");
        final MarcRecord deletedCommon = loadMarcRecord("deleted-870970.xml");
        final MarcRecord deletedMerged = loadMarcRecord("deleted-merged.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 870970));
        final String bibliographicRecordId = "00199087";

        final Record deletedEnrichmentRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichment, StandardCharsets.UTF_8));
        deletedEnrichmentRecord.setDeleted(true);
        deletedEnrichmentRecord.setCreated(getInstant("2016-01-01"));
        deletedEnrichmentRecord.setModified(getInstant("2017-01-01"));

        final Record deletedCommonRecord = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommon, StandardCharsets.UTF_8));
        deletedCommonRecord.setDeleted(true);

        deletedCommonRecord.setCreated(getInstant("2016-01-01"));
        deletedCommonRecord.setModified(getInstant("2017-01-01"));

        final Record expected = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedMerged, StandardCharsets.UTF_8));
        expected.setDeleted(true);
        expected.setCreated(getInstant("2016-01-01"));
        expected.setModified(getInstant("2017-01-01"));

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 191919)).thenReturn(false);
        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 870970)).thenReturn(false);
        when(recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919)).thenReturn(Collections.singleton(new RecordId(bibliographicRecordId, 870970)));
        when(recordRelationsBean.getRelationsSiblingsFromMe("69208045", 191919)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 870970)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, false)).thenThrow(new RawRepoExceptionRecordNotFound());
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);

        final Record mergedDeletedRecord = bean.getRawRepoRecordMerged(bibliographicRecordId, 191919, true, false, true);

        assertThat(mergedDeletedRecord.getId(), is(expected.getId()));
        assertThat(mergedDeletedRecord.isDeleted(), is(expected.isDeleted()));
        assertThat(mergedDeletedRecord.getMimeType(), is(expected.getMimeType()));
        assertThat(mergedDeletedRecord.getCreated(), is(expected.getCreated()));
        assertThat(mergedDeletedRecord.getModified(), is(expected.getModified()));
        assertThat(mergedDeletedRecord.getTrackingId(), is(expected.getTrackingId()));
        assertThat(mergedDeletedRecord.getEnrichmentTrail(), is("870970,191919"));

        // MarcXchange Reader and Writer does stuff to the XML namespace and structure, so in order to do a proper
        // comparison we have to run the out content through a reader and writer first.
        final InputStream inputStream = new ByteArrayInputStream(mergedDeletedRecord.getContent());
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        final MarcRecord mergedMarcRecord = reader.read();
        final byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);
        assertThat(mergedContent, is(expected.getContent()));
    }

    @Test
    public void testMergeDeletedRecord_ExcludeDBCFields() throws Exception {
        final RecordBean bean = initRecordBeanMock();

        final MarcRecord deletedEnrichment = loadMarcRecord("deleted-191919.xml");
        final MarcRecord deletedCommon = loadMarcRecord("deleted-870970.xml");
        final MarcRecord deletedMerged = loadMarcRecord("deleted-merged.xml");
        final Set<Integer> agenciesFound = new HashSet<>(Arrays.asList(191919, 870970));
        final String bibliographicRecordId = "00199087";

        final Record deletedEnrichmentRecord = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(deletedEnrichment, StandardCharsets.UTF_8));
        deletedEnrichmentRecord.setDeleted(true);
        deletedEnrichmentRecord.setCreated(getInstant("2016-01-01"));
        deletedEnrichmentRecord.setModified(getInstant("2017-01-01"));

        final Record deletedCommonRecord = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedCommon, StandardCharsets.UTF_8));
        deletedCommonRecord.setDeleted(true);

        deletedCommonRecord.setCreated(getInstant("2016-01-01"));
        deletedCommonRecord.setModified(getInstant("2017-01-01"));

        final Record expected = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(deletedMerged, StandardCharsets.UTF_8));
        expected.setDeleted(true);
        expected.setCreated(getInstant("2016-01-01"));
        expected.setModified(getInstant("2017-01-01"));

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 191919)).thenReturn(false);
        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 870970)).thenReturn(false);
        when(recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919)).thenReturn(Collections.singleton(new RecordId(bibliographicRecordId, 870970)));
        when(recordRelationsBean.getRelationsSiblingsFromMe("69208045", 191919)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 870970)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, false)).thenThrow(new RawRepoExceptionRecordNotFound());
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);

        final Record mergedDeletedRecord = bean.getRawRepoRecordMerged(bibliographicRecordId, 191919, true, true, true);

        assertThat(mergedDeletedRecord.getId(), is(expected.getId()));
        assertThat(mergedDeletedRecord.isDeleted(), is(expected.isDeleted()));
        assertThat(mergedDeletedRecord.getMimeType(), is(expected.getMimeType()));
        assertThat(mergedDeletedRecord.getCreated(), is(expected.getCreated()));
        assertThat(mergedDeletedRecord.getModified(), is(expected.getModified()));
        assertThat(mergedDeletedRecord.getTrackingId(), is(expected.getTrackingId()));
        assertThat(mergedDeletedRecord.getEnrichmentTrail(), is("870970,191919"));

        // MarcXchange Reader and Writer does stuff to the XML namespace and structure, so in order to do a proper
        // comparison we have to run the out content through a reader and writer first.
        final InputStream inputStream = new ByteArrayInputStream(mergedDeletedRecord.getContent());
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        final MarcRecord mergedMarcRecord = reader.read();
        final byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);
        assertThat(mergedContent, is(expected.getContent()));
    }


    @Test
    public void testMergeDeletedRecordExpanded() throws Exception {
        final RecordBean bean = initRecordBeanMock();

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

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 191919)).thenReturn(false);
        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 870970)).thenReturn(false);
        when(recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919)).thenReturn(Collections.singleton(new RecordId(bibliographicRecordId, 870970)));
        when(recordRelationsBean.getRelationsSiblingsFromMe("69208045", 191919)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 870970)).thenReturn(Collections.singleton(new RecordId("69208045", 870979)));
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agenciesFound);
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, false)).thenThrow(new RawRepoExceptionRecordNotFound());
        when(rawRepoDAO.agencyFor(bibliographicRecordId, 191919, true)).thenReturn(191919);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 191919)).thenReturn(deletedEnrichmentRecord);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(deletedCommonRecord);
        when(recordSimpleBean.fetchRecord("69208045", 870979)).thenReturn(authorityRecord);

        final Record actualRecord = bean.getRawRepoRecordExpanded(bibliographicRecordId, 191919, true, false, true, false);

        assertThat(actualRecord.getId(), is(expectedMergedRecord.getId()));
        assertThat(actualRecord.isDeleted(), is(expectedMergedRecord.isDeleted()));
        assertThat(actualRecord.getMimeType(), is(expectedMergedRecord.getMimeType()));
        assertThat(actualRecord.getCreated(), is(expectedMergedRecord.getCreated()));
        assertThat(actualRecord.getTrackingId(), is(expectedMergedRecord.getTrackingId()));
        assertThat(actualRecord.getEnrichmentTrail(), is("870970,191919"));

        // MarcXchange Reader and Writer does stuff to the XML namespace and structure, so in order to do a proper
        // comparison we have to run the out content through a reader and writer first.
        InputStream inputStream = new ByteArrayInputStream(actualRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord actualMarcRecord = reader.read();
        byte[] actualMarcRecordContent = marcXchangeV1Writer.write(actualMarcRecord, StandardCharsets.UTF_8);

        assertThat(new String(actualMarcRecordContent), is(new String(expectedExpandedRecord.getContent())));
    }

}
