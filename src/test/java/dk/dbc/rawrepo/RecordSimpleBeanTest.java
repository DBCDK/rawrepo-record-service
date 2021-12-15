package dk.dbc.rawrepo;

import dk.dbc.rawrepo.exception.RecordNotFoundException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import javax.sql.DataSource;
import java.sql.Connection;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.when;

public class RecordSimpleBeanTest {

    @Mock
    private DataSource globalDataSource;

    @Mock
    private RawRepoDAO rawRepoDAO;

    @Mock
    private static RelationHintsVipCore relationHints;

    private class RecordSimpleBeanMock extends RecordSimpleBean {
        RecordSimpleBeanMock(DataSource globalDataSource) {
            super(globalDataSource);

            this.relationHints = RecordSimpleBeanTest.relationHints;
        }

        @Override
        protected RawRepoDAO createDAO(Connection conn) {
            rawRepoDAO.relationHints = this.relationHints;

            return rawRepoDAO;
        }
    }

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void recordIsActiveTestActive() throws Exception {
        final RecordSimpleBean bean = new RecordSimpleBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(true);

        assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(true));
    }

    @Test
    public void recordIsActiveTestDeleted() throws Exception {
        final RecordSimpleBean bean = new RecordSimpleBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(true);
        when(rawRepoDAO.recordExists(bibliographicRecordId, agencyId)).thenReturn(false);

        assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(false));
    }

    @Test
    public void recordIsActiveTestNotFound() throws Exception {
        final RecordSimpleBean bean = new RecordSimpleBeanMock(globalDataSource);
        final String bibliographicRecordId = "12345678";
        final int agencyId = 870970;

        when(rawRepoDAO.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)).thenReturn(false);

        Assertions.assertThrows(RecordNotFoundException.class, () -> assertThat(bean.recordIsActive(bibliographicRecordId, agencyId), is(true)));
    }
}
