package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static dk.dbc.rawrepo.BeanTestHelper.createRecordMock;
import static dk.dbc.rawrepo.BeanTestHelper.getInstant;
import static dk.dbc.rawrepo.BeanTestHelper.loadMarcRecord;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class RecordCollectionBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RecordBean recordBean;

    @Mock
    private RecordRelationsBean recordRelationsBean;

    @Mock
    private static RelationHintsOpenAgency relationHintsOpenAgency;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    private class RecordCollectionBeanMock extends RecordCollectionBean {
        RecordCollectionBeanMock(DataSource globalDataSource, RecordBean recordBean, RecordRelationsBean recordRelationsBean) {
            super(globalDataSource);

            this.recordBean = recordBean;
            this.recordRelationsBean = recordRelationsBean;
        }
    }

    private RecordCollectionBeanMock initRecordCollectionBeanMock() {
        return new RecordCollectionBeanMock(globalDataSource, recordBean, recordRelationsBean);
    }

    @BeforeEach
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

    @Test
    public void testFetchRecordCollectionExpandedExistingPHRecord() throws Exception {
        final RecordCollectionBean bean = new RecordCollectionBeanMock(globalDataSource, recordBean, recordRelationsBean);

        final String bibliographicRecordId = "90004158";
        final int originalAgencyId = 700300;
        final int agencyId = 870970;
        final Map<String, Record> collection = new HashMap<>();
        final MarcRecord marcRecord = loadMarcRecord("merged-deleted/expected-expanded.xml");

        final Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));
        record.setDeleted(true);
        record.setCreated(getInstant("2018-09-11"));
        record.setModified(getInstant("2019-09-11"));

        collection.put(bibliographicRecordId, record);

        when(recordRelationsBean.findParentRelationAgency(bibliographicRecordId, originalAgencyId)).thenReturn(agencyId);
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, agencyId)).thenReturn(Collections.EMPTY_SET);
        when(recordBean.getRawRepoRecordMerged(bibliographicRecordId, originalAgencyId, true, false, true)).thenReturn(record);

        assertThat(bean.getRawRepoRecordCollection(bibliographicRecordId, originalAgencyId, true, false, true, false, false, false), is(collection));
    }

    @Test
    public void testFetchRecordCollectionExpandedExistingRecord() throws Exception {
        final RecordCollectionBean bean = initRecordCollectionBeanMock();

        final String bibliographicRecordId = "90004158";
        final int agencyId = 191919;
        final Map<String, Record> collection = new HashMap<>();
        final MarcRecord marcRecord = loadMarcRecord("merged-deleted/expected-expanded.xml");

        final Record record = createRecordMock(bibliographicRecordId, 191919, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));
        record.setDeleted(true);
        record.setCreated(getInstant("2018-09-11"));
        record.setModified(getInstant("2019-09-11"));

        collection.put(bibliographicRecordId, record);

        when(recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId)).thenReturn(agencyId);
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, agencyId)).thenReturn(Collections.EMPTY_SET);
        when(recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, true, false, true)).thenReturn(record);

        assertThat(bean.getRawRepoRecordCollection(bibliographicRecordId, agencyId, true, false, true, false, true, false), is(collection));
    }

    @Test
    public void testFetchRecordCollectionExistingRecord() throws Exception {
        final RecordCollectionBean bean = initRecordCollectionBeanMock();

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
        when(recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId)).thenReturn(agencyId);
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, agencyId)).thenReturn(Collections.EMPTY_SET);
        when(recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, true, false, false)).thenReturn(record);

        assertThat(bean.getRawRepoRecordCollection(bibliographicRecordId, agencyId, true, false, false, false, false, false), is(collection));
    }

    @Test
    public void testFetchRecordCollectionDeletedRecord() throws Exception {
        final RecordCollectionBean bean = initRecordCollectionBeanMock();

        final MarcRecord deletedEnrichmentMarcRecord = loadMarcRecord("merged-deleted/common-enrichment.xml");
        final MarcRecord deletedCommonMarcRecord = loadMarcRecord("merged-deleted/common-dbc.xml");
        final MarcRecord expectedMergedMarcRecord = loadMarcRecord("merged-deleted/expected-merged.xml");
        final MarcRecord authorityMarcRecord = loadMarcRecord("merged-deleted/aut-dbc.xml");
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

        final Record authorityRecord = createRecordMock(autBibliographicRecordId, 870979, MarcXChangeMimeType.AUTHORITY,
                marcXchangeV1Writer.write(authorityMarcRecord, StandardCharsets.UTF_8));
        authorityRecord.setDeleted(false);
        authorityRecord.setCreated(getInstant("2018-09-11"));
        authorityRecord.setModified(getInstant("2019-09-11"));

        when(recordBean.getRawRepoRecordMerged(bibliographicRecordId, 191919, true, false, false)).thenReturn(expectedMergedRecord);
        when(recordBean.getRawRepoRecordMerged(autBibliographicRecordId, 191919, true, false, false)).thenReturn(authorityRecord);
        when(recordRelationsBean.findParentRelationAgency(bibliographicRecordId, 191919)).thenReturn(870970);
        when(recordRelationsBean.findParentRelationAgency(autBibliographicRecordId, 191919)).thenReturn(870979);
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 191919)).thenReturn(Collections.singleton(new RecordId(autBibliographicRecordId, 870979)));
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 870970)).thenReturn(Collections.singleton(new RecordId(autBibliographicRecordId, 870979)));

        final Map<String, Record> actual = bean.getRawRepoRecordCollection(bibliographicRecordId, 191919, true, false, false, false, false, false);

        assertThat(actual.size(), is(2));

        assertTrue(actual.containsKey(bibliographicRecordId));
        assertTrue(actual.containsKey(autBibliographicRecordId));

        final Record actualCommonRecord = actual.get(bibliographicRecordId);

        assertThat(actualCommonRecord.getId(), is(expectedMergedRecord.getId()));
        assertThat(actualCommonRecord.isDeleted(), is(expectedMergedRecord.isDeleted()));
        assertThat(actualCommonRecord.getMimeType(), is(expectedMergedRecord.getMimeType()));
        assertThat(actualCommonRecord.getCreated(), is(expectedMergedRecord.getCreated()));
        assertThat(actualCommonRecord.getModified(), is(expectedMergedRecord.getModified()));
        assertThat(actualCommonRecord.getTrackingId(), is(expectedMergedRecord.getTrackingId()));

        InputStream inputStream = new ByteArrayInputStream(actualCommonRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord mergedMarcRecord = reader.read();
        byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        assertThat(mergedContent, is(expectedMergedRecord.getContent()));

        final Record actualAutRecord = actual.get(autBibliographicRecordId);

        assertThat(actualAutRecord.getId(), is(authorityRecord.getId()));
        assertThat(actualAutRecord.isDeleted(), is(authorityRecord.isDeleted()));
        assertThat(actualAutRecord.getMimeType(), is(authorityRecord.getMimeType()));
        assertThat(actualAutRecord.getCreated(), is(authorityRecord.getCreated()));
        assertThat(actualAutRecord.getModified(), is(authorityRecord.getModified()));
        assertThat(actualAutRecord.getTrackingId(), is(authorityRecord.getTrackingId()));

        inputStream = new ByteArrayInputStream(actualAutRecord.getContent());
        bufferedInputStream = new BufferedInputStream(inputStream);
        reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        mergedMarcRecord = reader.read();
        mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        assertThat(mergedContent, is(authorityRecord.getContent()));
    }

    @Test
    public void testFetchRecordCollectionExpandedDeletedRecord() throws Exception {
        final RecordCollectionBean bean = initRecordCollectionBeanMock();

        final MarcRecord deletedEnrichmentMarcRecord = loadMarcRecord("merged-deleted/common-enrichment.xml");
        final MarcRecord deletedCommonMarcRecord = loadMarcRecord("merged-deleted/common-dbc.xml");
        final MarcRecord expanded = loadMarcRecord("merged-deleted/expected-expanded.xml");
        final MarcRecord authorityMarcRecord = loadMarcRecord("merged-deleted/aut-dbc.xml");
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

        when(recordBean.getRawRepoRecordExpanded(bibliographicRecordId, 191919, true, false, true, false)).thenReturn(expectedExpandedRecord);
        when(recordBean.getRawRepoRecordExpanded(autBibliographicRecordId, 191919, true, false, true, false)).thenReturn(authorityRecord);
        when(recordRelationsBean.findParentRelationAgency(bibliographicRecordId, 191919)).thenReturn(870970);
        when(recordRelationsBean.findParentRelationAgency(autBibliographicRecordId, 191919)).thenReturn(870979);
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 191919)).thenReturn(Collections.singleton(new RecordId(autBibliographicRecordId, 870979)));
        when(recordRelationsBean.getRelationsParents(bibliographicRecordId, 870970)).thenReturn(Collections.singleton(new RecordId(autBibliographicRecordId, 870979)));

        final Map<String, Record> actual = bean.getRawRepoRecordCollection(bibliographicRecordId, 191919, true, false, true, true, false, false);

        assertThat(actual.size(), is(2));
        assertTrue(actual.containsKey(bibliographicRecordId));
        assertTrue(actual.containsKey(autBibliographicRecordId));

        final Record actualCommonRecord = actual.get(bibliographicRecordId);

        assertThat(actualCommonRecord.getId(), is(expectedExpandedRecord.getId()));
        assertThat(actualCommonRecord.isDeleted(), is(expectedExpandedRecord.isDeleted()));
        assertThat(actualCommonRecord.getMimeType(), is(expectedExpandedRecord.getMimeType()));
        assertThat(actualCommonRecord.getCreated(), is(expectedExpandedRecord.getCreated()));
        // We won't compare modified date as it is set during expandRecord. Therefore we can't compare to a static value
        assertThat(actualCommonRecord.getTrackingId(), is(expectedExpandedRecord.getTrackingId()));

        InputStream inputStream = new ByteArrayInputStream(actualCommonRecord.getContent());
        BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        MarcRecord mergedMarcRecord = reader.read();
        byte[] mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        assertThat(mergedContent, is(expectedExpandedRecord.getContent()));

        final Record actualAutRecord = actual.get(autBibliographicRecordId);

        assertThat(actualAutRecord.getId(), is(authorityRecord.getId()));
        assertThat(actualAutRecord.isDeleted(), is(authorityRecord.isDeleted()));
        assertThat(actualAutRecord.getMimeType(), is(authorityRecord.getMimeType()));
        assertThat(actualAutRecord.getCreated(), is(authorityRecord.getCreated()));
        assertThat(actualAutRecord.getModified(), is(authorityRecord.getModified()));
        assertThat(actualAutRecord.getTrackingId(), is(authorityRecord.getTrackingId()));

        inputStream = new ByteArrayInputStream(actualAutRecord.getContent());
        bufferedInputStream = new BufferedInputStream(inputStream);
        reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);
        mergedMarcRecord = reader.read();
        mergedContent = marcXchangeV1Writer.write(mergedMarcRecord, StandardCharsets.UTF_8);

        assertThat(mergedContent, is(authorityRecord.getContent()));
    }
}
