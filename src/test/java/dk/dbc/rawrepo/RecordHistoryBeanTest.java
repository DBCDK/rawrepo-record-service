package dk.dbc.rawrepo;

import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

import static dk.dbc.rawrepo.BeanTestHelper.getInstant;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

public class RecordHistoryBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    @Mock
    private static RelationHintsOpenAgency relationHintsOpenAgency;

    private final MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();

    private class RecordHistoryBeanMock extends RecordHistoryBean {
        RecordHistoryBeanMock(DataSource globalDataSource) {
            super(globalDataSource);
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

            return rawRepoDAO;
        }
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testGetRecordHistory() throws Exception {
        final RecordHistoryBean recordHistoryBean = new RecordHistoryBeanMock(globalDataSource);

        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;
        final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

        final List<RecordMetaDataHistory> mockedRecordMetaDataHistories = new ArrayList<>();
        mockedRecordMetaDataHistories.add(new RecordMetaDataHistory(recordId, false, MarcXChangeMimeType.MARCXCHANGE, getInstant("2020-01-01"), getInstant("2020-01-01"), ""));
        mockedRecordMetaDataHistories.add(new RecordMetaDataHistory(recordId, true, MarcXChangeMimeType.MARCXCHANGE, getInstant("2020-01-01"), getInstant("2020-02-13"), ""));

        when(rawRepoDAO.getRecordHistory(bibliographicRecordId, agencyId)).thenReturn(mockedRecordMetaDataHistories);

        final List<RecordMetaDataHistory> metaDataHistories = recordHistoryBean.getRecordHistory(bibliographicRecordId, agencyId);

        assertThat(metaDataHistories.size(), is(2));

        assertThat(metaDataHistories.get(0).getId(), is(recordId));
        assertThat(metaDataHistories.get(0).isDeleted(), is(false));
        assertThat(metaDataHistories.get(0).getMimeType(), is(MarcXChangeMimeType.MARCXCHANGE));
        assertThat(metaDataHistories.get(0).getCreated(), is(getInstant("2020-01-01")));
        assertThat(metaDataHistories.get(0).getModified(), is(getInstant("2020-01-01")));

        assertThat(metaDataHistories.get(1).getId(), is(recordId));
        assertThat(metaDataHistories.get(1).isDeleted(), is(true));
        assertThat(metaDataHistories.get(1).getMimeType(), is(MarcXChangeMimeType.MARCXCHANGE));
        assertThat(metaDataHistories.get(1).getCreated(), is(getInstant("2020-01-01")));
        assertThat(metaDataHistories.get(1).getModified(), is(getInstant("2020-02-13")));
    }

}
