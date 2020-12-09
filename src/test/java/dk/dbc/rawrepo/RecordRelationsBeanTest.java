/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static dk.dbc.rawrepo.BeanTestHelper.createRecordMock;
import static dk.dbc.rawrepo.BeanTestHelper.loadMarcRecord;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.when;

public class RecordRelationsBeanTest {

    @Mock
    DataSource globalDataSource;

    @Mock
    RawRepoDAO rawRepoDAO;

    @Mock
    RecordSimpleBean recordSimpleBean;

    @Mock
    private static RelationHintsVipCore relationHints;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();
    private final String COMMON = "common";
    private final String ARTICLE = "article";
    private final String AUTHORITY = "authority";
    private final String LITTOLK = "littolk";

    private class RecordRelationsBeanMock extends RecordRelationsBean {
        RecordRelationsBeanMock(DataSource globalDataSource, RecordSimpleBean recordSimpleBean) {
            super(globalDataSource);

            this.relationHints = RecordRelationsBeanTest.relationHints;
            this.recordSimpleBean = recordSimpleBean;
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

            return rawRepoDAO;
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);

        doCallRealMethod().when(relationHints).getAgencyPriority(anyInt());
        doCallRealMethod().when(relationHints).usesCommonSchoolAgency(anyInt());
        doCallRealMethod().when(relationHints).get(anyInt());
        when(relationHints.usesCommonAgency(anyInt())).thenReturn(false);
        when(relationHints.usesCommonAgency(eq(191919))).thenReturn(true);
        when(relationHints.usesCommonAgency(eq(700300))).thenReturn(true);
        when(relationHints.usesCommonAgency(eq(870970))).thenReturn(true);
        when(relationHints.usesCommonAgency(eq(870971))).thenReturn(true);
        when(relationHints.usesCommonAgency(eq(870974))).thenReturn(true);
        when(relationHints.usesCommonAgency(eq(870979))).thenReturn(true);
    }

    @Test
    public void testGetAllAgenciesForBibliographicRecordId() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);
        String bibliographicRecordId = "123456789";

        Set<Integer> agencySet = new HashSet<>(Arrays.asList(191919, 870970));

        when(globalDataSource.getConnection()).thenReturn(null);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(eq(bibliographicRecordId))).thenReturn(agencySet);

        assertThat(bean.getAllAgenciesForBibliographicRecordId(bibliographicRecordId), is(agencySet));
    }

    @Test
    public void findParentRelationAgencyTestActive() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(recordSimpleBean.recordExists(anyString(), eq(191919), eq(true))).thenReturn(true);
        when(recordSimpleBean.recordExists(COMMON, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(ARTICLE, 870971, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(AUTHORITY, 870979, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(LITTOLK, 870974, true)).thenReturn(true);

        when(recordSimpleBean.recordExists(anyString(), eq(191919), eq(false))).thenReturn(true);
        when(recordSimpleBean.recordExists(COMMON, 870970, false)).thenReturn(true);
        when(recordSimpleBean.recordExists(ARTICLE, 870971, false)).thenReturn(true);
        when(recordSimpleBean.recordExists(LITTOLK, 870974, false)).thenReturn(true);
        when(recordSimpleBean.recordExists(AUTHORITY, 870979, false)).thenReturn(true);

        assertFindParentRelationAgency(bean);
    }

    @Test
    public void findParentRelationAgencyTestNotActive() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(recordSimpleBean.recordExists(anyString(), eq(191919), eq(true))).thenReturn(true);
        when(recordSimpleBean.recordExists(COMMON, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(ARTICLE, 870971, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(AUTHORITY, 870979, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(LITTOLK, 870974, true)).thenReturn(true);

        when(recordSimpleBean.recordExists(anyString(), eq(191919), eq(false))).thenReturn(false);
        when(recordSimpleBean.recordExists(COMMON, 870970, false)).thenReturn(false);
        when(recordSimpleBean.recordExists(ARTICLE, 870971, false)).thenReturn(false);
        when(recordSimpleBean.recordExists(LITTOLK, 870974, false)).thenReturn(false);
        when(recordSimpleBean.recordExists(AUTHORITY, 870979, false)).thenReturn(false);

        assertFindParentRelationAgency(bean);
    }


    @Test
    public void findParentRelationAgencyFBSLocal() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        doCallRealMethod().when(rawRepoDAO).findParentRelationAgency(anyString(), anyInt());

        when(recordSimpleBean.recordExists(anyString(), eq(820010), eq(true))).thenReturn(true);

        assertThat(bean.findParentRelationAgency("FBS", 820010), is(820010));
    }

    @Test
    public void testGetRelationsParents191919() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(COMMON, 191919)).thenReturn(false);
        when(rawRepoDAO.recordExistsMaybeDeleted(COMMON, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(COMMON, 870970)).thenReturn(false);

        Set<RecordId> actual = bean.getRelationsParents(COMMON, 191919);

        assertThat(actual.size(), is(0));
    }


    @Test
    public void testGetRelationsParentsFBSEnrichment() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "50938409";

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 911116)).thenReturn(true);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 911116);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsParentsFBSLocal() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "2207787";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/fbs-local.xml");

        final Record record = createRecordMock(bibliographicRecordId, 820010, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 820010)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 820010)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 820010);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("223906", 820010)));
    }

    @Test
    public void testGetRelationsParentsCommonNoParents() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "50938409";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-no-relations.xml");

        final Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsParentsCommonSingleAut() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "50938409";

        final MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-single-aut.xml");

        final Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);
        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 870970)).thenReturn(false);
        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("69208045", 870979)));
    }

    @Test
    public void testGetRelationsParentsCommonTripleAut() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "53090567";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-triple-aut.xml");

        Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(4));

        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("19050416", 870979)));
        assertThat(iterator.next(), is(new RecordId("19050785", 870979)));
        assertThat(iterator.next(), is(new RecordId("19047903", 870979)));
        assertThat(iterator.next(), is(new RecordId("69208045", 870979)));
    }

    @Test
    public void testGetRelationsParentsArticle() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "85803190";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/article.xml");

        Record record = createRecordMock(bibliographicRecordId, 870971, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870971)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870971)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870971);

        assertThat(actual.size(), is(1));

        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("23191083", 870970)));
    }

    @Test
    public void testGetRelationsParentsVolume() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "22723715";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/common-volume.xml");

        Record record = createRecordMock(bibliographicRecordId, 870970, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870970)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(1));

        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("50434990", 870970)));
    }

    @Test
    public void testGetRelationsParentsLittolk() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "126395604";

        MarcRecord marcRecord = loadMarcRecord("getRelationsParents/littolk.xml");

        Record record = createRecordMock(bibliographicRecordId, 870974, MarcXChangeMimeType.MARCXCHANGE,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordSimpleBean.fetchRecord(bibliographicRecordId, 870974)).thenReturn(record);
        when(rawRepoDAO.fetchRecord(bibliographicRecordId, 870974)).thenReturn(record);

        Set<RecordId> actual = bean.getRelationsParents(bibliographicRecordId, 870974);

        assertThat(actual.size(), is(3));

        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId("68754011", 870979)));
        assertThat(iterator.next(), is(new RecordId("68234190", 870979)));
        assertThat(iterator.next(), is(new RecordId("46912683", 870970)));
    }

    @Test
    public void testGetRelationsSiblingsFromMeActiveRecord191919() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 191919);
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsFromMe(eq(thisRecordId))).thenReturn(new HashSet<>(Collections.singletonList(siblingFromMeRecordId)));
        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 191919)).thenReturn(true);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeActiveRecord870970() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 870970);


        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsFromMe(eq(thisRecordId))).thenReturn(new HashSet<>());

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive191919() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 191919);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive700300() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId siblingFromMeRecordId = new RecordId(bibliographicRecordId, 870970);
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 700300)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 700300)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 700300);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(siblingFromMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsFromMeInactive870970() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsFromMe(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeActiveRecord191919() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 191919);

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsToMe(eq(thisRecordId))).thenReturn(new HashSet<>());

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 191919);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeActiveRecord870970() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final RecordId thisRecordId = new RecordId(bibliographicRecordId, 870970);

        final RecordId siblingToMeRecordId = new RecordId(bibliographicRecordId, 191919);

        when(recordSimpleBean.recordIsActive(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.getRelationsSiblingsToMe(eq(thisRecordId))).thenReturn(new HashSet<>(Collections.singletonList(siblingToMeRecordId)));

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(1));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(siblingToMeRecordId));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive191919() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 191919)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 191919)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 191919);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive700300() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 700300)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 700300)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 700300);

        assertThat(actual.size(), is(0));
    }

    @Test
    public void testGetRelationsSiblingsToMeInactive870970() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordId = "12345678";
        final Set<Integer> allAgenciesForRecord = new HashSet<>(Arrays.asList(191919, 700300, 870970));

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, 870970)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, 870970)).thenReturn(false);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordId)).thenReturn(allAgenciesForRecord);

        Set<RecordId> actual = bean.getRelationsSiblingsToMe(bibliographicRecordId, 870970);

        assertThat(actual.size(), is(2));
        Iterator<RecordId> iterator = actual.iterator();
        assertThat(iterator.next(), is(new RecordId(bibliographicRecordId, 191919)));
        assertThat(iterator.next(), is(new RecordId(bibliographicRecordId, 700300)));
    }

    private void assertFindParentRelationAgency(RecordRelationsBean bean) throws Exception {
        assertThat(bean.findParentRelationAgency(COMMON, 191919), is(870970));
        assertThat(bean.findParentRelationAgency(COMMON, 870970), is(870970));

        assertThat(bean.findParentRelationAgency(ARTICLE, 191919), is(870971));
        assertThat(bean.findParentRelationAgency(ARTICLE, 870971), is(870971));

        assertThat(bean.findParentRelationAgency(LITTOLK, 191919), is(870974));
        assertThat(bean.findParentRelationAgency(LITTOLK, 870974), is(870974));

        assertThat(bean.findParentRelationAgency(AUTHORITY, 191919), is(870979));
        assertThat(bean.findParentRelationAgency(AUTHORITY, 870979), is(870979));
    }

    @Test
    void parentIsActiveTest_SameAgencyParent_Active() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordIdVolume = "B";
        final String bibliographicRecordIdHead = "H";
        final int agencyIdVolume = 700300;
        final Set<RecordId> parents = Collections.singleton(new RecordId(bibliographicRecordIdHead, 870970));

        when(recordSimpleBean.recordExists(bibliographicRecordIdVolume, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, 870970, true)).thenReturn(true);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdVolume, 870970))).thenReturn(parents);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdHead, 870970))).thenReturn(Collections.EMPTY_SET);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdVolume, 870970)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, 870970)).thenReturn(true);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordIdHead)).thenReturn(new HashSet<>(Arrays.asList(191919, 870970, agencyIdVolume)));
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, agencyIdVolume, true)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, agencyIdVolume)).thenReturn(true);

        assertTrue(bean.parentIsActive(bibliographicRecordIdVolume, agencyIdVolume));
    }

    @Test
    void parentIsActiveTest_SameAgencyParent_Deleted() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordIdVolume = "B";
        final String bibliographicRecordIdHead = "H";
        final int agencyIdVolume = 700300;
        final Set<RecordId> parents = Collections.singleton(new RecordId(bibliographicRecordIdHead, 870970));

        when(recordSimpleBean.recordExists(bibliographicRecordIdVolume, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, 870970, true)).thenReturn(true);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdVolume, 870970))).thenReturn(parents);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdHead, 870970))).thenReturn(Collections.EMPTY_SET);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdVolume, 870970)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, 870970)).thenReturn(true);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordIdHead)).thenReturn(new HashSet<>(Arrays.asList(191919, 870970, agencyIdVolume)));
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, agencyIdVolume, true)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, agencyIdVolume)).thenReturn(false);

        assertFalse(bean.parentIsActive(bibliographicRecordIdVolume, agencyIdVolume));
    }

    @Test
    void parentIsActiveTest_SameAgencyParent_Section_Active() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordIdVolume = "B";
        final String bibliographicRecordIdSection = "S";
        final String bibliographicRecordIdHead = "H";
        final int agencyIdVolume = 700300;
        final Set<RecordId> parentsVolume = Collections.singleton(new RecordId(bibliographicRecordIdSection, 870970));
        final Set<RecordId> parentsSection = Collections.singleton(new RecordId(bibliographicRecordIdHead, 870970));

        when(recordSimpleBean.recordExists(bibliographicRecordIdVolume, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(bibliographicRecordIdSection, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, 870970, true)).thenReturn(true);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdVolume, 870970))).thenReturn(parentsVolume);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdSection, 870970))).thenReturn(parentsSection);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdHead, 870970))).thenReturn(Collections.EMPTY_SET);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdVolume, 870970)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdSection, 870970)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, 870970)).thenReturn(true);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordIdSection)).thenReturn(new HashSet<>(Arrays.asList(191919, 870970)));
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordIdHead)).thenReturn(new HashSet<>(Arrays.asList(191919, 870970, agencyIdVolume)));
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, agencyIdVolume, true)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, agencyIdVolume)).thenReturn(true);

        assertTrue(bean.parentIsActive(bibliographicRecordIdVolume, agencyIdVolume));
    }

    @Test
    void parentIsActiveTest_NoSameAgencyParent() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordIdVolume = "B";
        final String bibliographicRecordIdHead = "H";
        final int agencyIdVolume = 700300;
        final Set<RecordId> parents = Collections.singleton(new RecordId(bibliographicRecordIdHead, 870970));

        when(recordSimpleBean.recordExists(bibliographicRecordIdVolume, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, 870970, true)).thenReturn(true);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdVolume, 870970))).thenReturn(parents);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdHead, 870970))).thenReturn(Collections.EMPTY_SET);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdVolume, 870970)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, 870970)).thenReturn(true);
        when(rawRepoDAO.allAgenciesForBibliographicRecordId(bibliographicRecordIdHead)).thenReturn(new HashSet<>(Arrays.asList(191919, 870970)));
        when(recordSimpleBean.recordExists(bibliographicRecordIdHead, 870970, true)).thenReturn(true);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdHead, 870970)).thenReturn(true);

        assertFalse(bean.parentIsActive(bibliographicRecordIdVolume, agencyIdVolume));
    }

    @Test
    void parentIsActiveTest_NoParents() throws Exception {
        final RecordRelationsBean bean = new RecordRelationsBeanMock(globalDataSource, recordSimpleBean);

        final String bibliographicRecordIdVolume = "B";
        final int agencyIdVolume = 700300;

        when(recordSimpleBean.recordExists(bibliographicRecordIdVolume, 870970, true)).thenReturn(true);
        when(rawRepoDAO.getRelationsParents(new RecordId(bibliographicRecordIdVolume, 870970))).thenReturn(Collections.EMPTY_SET);
        when(recordSimpleBean.recordIsActive(bibliographicRecordIdVolume, 870970)).thenReturn(true);

        assertFalse(bean.parentIsActive(bibliographicRecordIdVolume, agencyIdVolume));
    }

}
