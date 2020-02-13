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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static dk.dbc.rawrepo.BeanTestHelper.createRecordMock;
import static dk.dbc.rawrepo.BeanTestHelper.loadMarcRecord;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class MarcRecordBeanTest {
    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    @Mock
    private RecordBean recordBean;

    @Mock
    private RecordCollectionBean recordCollectionBean;

    @Mock
    private static RelationHintsOpenAgency relationHintsOpenAgency;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    private class MarcRecordBeanMock extends MarcRecordBean {
        MarcRecordBeanMock(DataSource globalDataSource, RecordBean recordBean, RecordCollectionBean recordCollectionBean) {
            super(globalDataSource);

            this.relationHints = relationHintsOpenAgency;
            this.recordBean = recordBean;
            this.recordCollectionBean = recordCollectionBean;
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

            return rawRepoDAO;
        }
    }

    private MarcRecordBeanMock initMarcRecordBeanMock() {
        return new MarcRecordBeanMock(globalDataSource, recordBean, recordCollectionBean);
    }

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetMergedRecordOk() throws Exception {
        MarcRecordBean bean = initMarcRecordBeanMock();

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, true, false, false)).thenReturn(record);

        assertThat(bean.getMarcRecordMerged(bibliographicRecordId, agencyId, true, false, false), is(marcRecord));
    }

    @Test
    public void testGetExpandedRecordOk() throws Exception {
        MarcRecordBean bean = initMarcRecordBeanMock();

        String bibliographicRecordId = "90004158";
        int agencyId = 191919;

        MarcRecord marcRecord = loadMarcRecord("merged.xml");

        Record record = createRecordMock(bibliographicRecordId, agencyId, MarcXChangeMimeType.ENRICHMENT,
                marcXchangeV1Writer.write(marcRecord, StandardCharsets.UTF_8));

        when(recordBean.getRawRepoRecordExpanded(bibliographicRecordId, agencyId, true, false, false, true)).thenReturn(record);

        assertThat(bean.getMarcRecordExpanded(bibliographicRecordId, agencyId, true, false, false, true), is(marcRecord));
    }

}
