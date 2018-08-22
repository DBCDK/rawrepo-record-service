/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.io.BufferedInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.CoreMatchers.is;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
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
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

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
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

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
        when(rawRepoDAO.fetchMergedRecord(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class), eq(true))).thenReturn(record);

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
        when(rawRepoDAO.fetchRecordCollection(eq(bibliographicRecordId), eq(agencyId), any(MarcXMerger.class))).thenReturn(recordMap);

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
}
