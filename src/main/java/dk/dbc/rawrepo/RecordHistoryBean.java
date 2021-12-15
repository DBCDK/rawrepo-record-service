package dk.dbc.rawrepo;

import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

@Stateless
public class RecordHistoryBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordHistoryBean.class);

    @Inject
    private VipCoreLibraryRulesConnector libraryRulesConnector;

    RelationHintsVipCore relationHints;

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        final RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
        rawRepoBuilder.relationHints(relationHints);
        return rawRepoBuilder.build();
    }

    // Constructor used for mocking
    RecordHistoryBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    public RecordHistoryBean() {

    }

    @PostConstruct
    public void init() {
        try {
            relationHints = new RelationHintsVipCore(libraryRulesConnector);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws
            InternalServerException {
        List<RecordMetaDataHistory> result;
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.getRecordHistory(bibliographicRecordId, agencyId);

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Record getHistoricRecord(RecordMetaDataHistory recordMetaDataHistory) throws InternalServerException {
        Record result;

        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.getHistoricRecord(recordMetaDataHistory);

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

}
