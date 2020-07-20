package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class RawRepoQueueBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoQueueBean.class);

    private static final String QUERY_QUEUE_RULES = "SELECT provider, worker, changed, leaf, description FROM queuerules ORDER BY provider, worker";
    private static final String QUERY_QUEUE_PROVIDERS = "SELECT distinct(provider) FROM queuerules ORDER BY provider";
    private static final String QUERY_QUEUE_WORKERS = "SELECT worker FROM queueworkers ORDER BY worker";

    private static final String ENQUEUE_AGENCY = "INSERT INTO queue SELECT bibliographicrecordid, ?, ?, now(), ? FROM records WHERE agencyid=?";

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    private OpenAgencyBean openAgency;

    RelationHintsOpenAgency relationHints;

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        final RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
        rawRepoBuilder.relationHints(relationHints);
        return rawRepoBuilder.build();
    }

    @PostConstruct
    public void init() {
        try {
            relationHints = new RelationHintsOpenAgency(openAgency.getService());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<QueueRuleDTO> getQueueRules() throws RawRepoException {
        List<QueueRuleDTO> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(QUERY_QUEUE_RULES)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String provider = resultSet.getString(1);
                    String worker = resultSet.getString(2);
                    char changed = resultSet.getString(3).charAt(0);
                    char leaf = resultSet.getString(4).charAt(0);
                    String description = resultSet.getString(5);

                    final QueueRuleDTO queueRuleDTO = new QueueRuleDTO(provider, worker, changed, leaf, description);
                    setQueueRuleDescription(queueRuleDTO);
                    result.add(queueRuleDTO);
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new RawRepoException("Error fetching queue rules", ex);
        }
    }

    private void setQueueRuleDescription(QueueRuleDTO queueRuleDTO) {
        if (queueRuleDTO.getDescription() == null) {
            final String changedLeafString = "" + queueRuleDTO.getChanged() + queueRuleDTO.getLeaf();

            switch(changedLeafString) {
                case "NN":
                    queueRuleDTO.setDescription("Hoved/Sektionsposter som er afhængige af den rørte post og ikke er rørt");
                    break;
                case "NY":
                    queueRuleDTO.setDescription("Bind/Enkeltstående poster som er afhængige af den rørte post og ikke er rørt");
                    break;
                case "NA":
                    queueRuleDTO.setDescription("Alle poster som er afhængige af den rørte post");
                    break;
                case "YN":
                    queueRuleDTO.setDescription("Den rørte post, hvis det er en Hoved/Sektionsport");
                    break;
                case "YY":
                    queueRuleDTO.setDescription("Den rørte post, hvis det er en Bind/Enkeltstående post");
                    break;
                case "YA":
                    queueRuleDTO.setDescription("Den rørte post");
                    break;
                case "AN":
                    queueRuleDTO.setDescription("Alle Hoved/Sektionsposter som er afhængige af den rørte post incl den rørte post");
                    break;
                case "AY":
                    queueRuleDTO.setDescription("Alle Bind/Enkeltstående poster som er afhængige af den rørte post incl den rørte post");
                    break;
                case "AA":
                    queueRuleDTO.setDescription("Den rørte post og alle poster som er afhængige af den");
                    break;
                default:
                    queueRuleDTO.setDescription("Ukendt kombination");
                    break;
            }
        }
    }

    public List<String> getQueueProviders() throws RawRepoException{
        List<String> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(QUERY_QUEUE_PROVIDERS)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String provider = resultSet.getString(1);

                    result.add(provider);
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new RawRepoException("Error fetching queue providers", ex);
        }
    }

    public List<String> getQueueWorkers() throws RawRepoException{
        List<String> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(QUERY_QUEUE_WORKERS)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    String worker = resultSet.getString(1);

                    result.add(worker);
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new RawRepoException("Error fetching queue workers", ex);
        }
    }

    public void enqueueRecord(String bibliographicRecordId, int agencyId, String provider, boolean changed, boolean leaf)
            throws RawRepoException, InternalServerException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                dao.enqueue(recordId, provider, changed, leaf);
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new RawRepoException(ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public void enqueueRecord(String bibliographicRecordId, int agencyId, String provider, boolean changed, boolean leaf, int priority)
            throws RawRepoException, InternalServerException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                dao.enqueue(recordId, provider, changed, leaf, priority);
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new RawRepoException(ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public int enqueueAgency(int agencyId, String worker, int priority) throws RawRepoException {
        return enqueueAgency(agencyId, agencyId, worker, priority);
    }

    public int enqueueAgency(int selectAgencyId, int queueAgencyId, String worker, int priority) throws RawRepoException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(ENQUEUE_AGENCY)) {
            int pos = 1;
            stmt.setInt(pos++, queueAgencyId);
            stmt.setString(pos++, worker);
            stmt.setInt(pos++, priority);
            stmt.setInt(pos, selectAgencyId);

            return stmt.executeUpdate();
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new RawRepoException("Error when queue agency", ex);
        }
    }
}
