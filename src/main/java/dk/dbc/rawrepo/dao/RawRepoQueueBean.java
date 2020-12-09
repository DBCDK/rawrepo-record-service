/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.rawrepo.dto.EnqueueResultDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueStatDTO;
import dk.dbc.rawrepo.exception.QueueException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class RawRepoQueueBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoQueueBean.class);

    private static final String SELECT_FROM_QUEUERULES = "SELECT provider, worker, changed, leaf, description FROM queuerules ORDER BY provider, worker";
    private static final String SELECT_DISTINCT_PROVIDER_FROM_QUEUERULES = "SELECT distinct(provider) FROM queuerules ORDER BY provider";
    private static final String SELECT_WORKER_FROM_QUEUEWORKERS = "SELECT worker FROM queueworkers ORDER BY worker";
    private static final String SELECT_QUEUE_COUNT_BY_WORKER = "SELECT worker AS text, COUNT(*), MAX(queued) FROM queue GROUP BY worker ORDER BY worker";
    private static final String SELECT_QUEUE_COUNT_BY_AGENCY = "SELECT agencyid AS text, COUNT(*), MAX(queued) FROM queue GROUP BY agencyid ORDER BY agencyid";

    private static final String ENQUEUE_AGENCY = "INSERT INTO queue SELECT bibliographicrecordid, ?, ?, now(), ? FROM records WHERE agencyid=?";
    private static final String CALL_ENQUEUE = "SELECT * FROM enqueue(?, ?, ?, ?, ?, ?)";

    private static final String DELETE_RECORD_CACHE = "DELETE FROM records_cache WHERE bibliographicrecordid=? AND agencyid=?";

    private static final String LOG_DATABASE_ERROR = "Error accessing database";

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @Inject
    VipCoreLibraryRulesConnector libraryRulesConnector;

    RelationHintsVipCore relationHints;

    @PostConstruct
    public void init() {
        try {
            relationHints = new RelationHintsVipCore(libraryRulesConnector);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public List<QueueRuleDTO> getQueueRules() throws QueueException {
        List<QueueRuleDTO> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(SELECT_FROM_QUEUERULES)) {
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
            throw new QueueException("Error fetching queue rules", ex);
        }
    }

    private void setQueueRuleDescription(QueueRuleDTO queueRuleDTO) {
        if (queueRuleDTO.getDescription() == null) {
            final String changedLeafString = "" + queueRuleDTO.getChanged() + queueRuleDTO.getLeaf();

            switch (changedLeafString) {
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

    public List<String> getQueueProviders() throws QueueException {
        final List<String> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(SELECT_DISTINCT_PROVIDER_FROM_QUEUERULES)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String provider = resultSet.getString(1);

                    result.add(provider);
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new QueueException("Error fetching queue providers", ex);
        }
    }

    public List<String> getQueueWorkers() throws QueueException {
        final List<String> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(SELECT_WORKER_FROM_QUEUEWORKERS)) {
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String worker = resultSet.getString(1);

                    result.add(worker);
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new QueueException("Error fetching queue workers", ex);
        }
    }

    public List<EnqueueResultDTO> enqueueRecord(String bibliographicRecordId, int agencyId, String provider, boolean changed, boolean leaf, int priority) throws QueueException {
        final List<EnqueueResultDTO> res = new ArrayList<>();

        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(CALL_ENQUEUE)) {
            int pos = 1;
            stmt.setString(pos++, bibliographicRecordId);
            stmt.setInt(pos++, agencyId);
            stmt.setString(pos++, provider);
            stmt.setString(pos++, changed ? "Y" : "N");
            stmt.setString(pos++, leaf ? "Y" : "N");
            stmt.setInt(pos, priority);
            try (ResultSet resultSet = stmt.executeQuery()) {
                while (resultSet.next()) {
                    final String worker = resultSet.getString(1);
                    final boolean queued = resultSet.getBoolean(2);
                    if (queued) {
                        LOGGER.info("Queued: worker = {}; job = {}:{}", resultSet.getString(1), bibliographicRecordId, agencyId);
                    } else {
                        LOGGER.info("Queued: worker = {}; job = {}:{}; skipped - already on queue", resultSet.getString(1), bibliographicRecordId, agencyId);
                    }
                    res.add(new EnqueueResultDTO(bibliographicRecordId, agencyId, worker, queued));
                }
            }
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error queueing job", ex);
        }
        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(DELETE_RECORD_CACHE)) {
            stmt.setString(1, bibliographicRecordId);
            stmt.setInt(2, agencyId);
            stmt.execute();
        } catch (SQLException ex) {
            LOGGER.error(LOG_DATABASE_ERROR, ex);
            throw new QueueException("Error deleting cache", ex);
        }

        return res;
    }

    public int enqueueAgency(int agencyId, String worker, int priority) throws QueueException {
        return enqueueAgency(agencyId, agencyId, worker, priority);
    }

    public int enqueueAgency(int selectAgencyId, int queueAgencyId, String worker, int priority) throws QueueException {
        try (Connection connection = dataSource.getConnection(); PreparedStatement stmt = connection.prepareStatement(ENQUEUE_AGENCY)) {
            int pos = 1;
            stmt.setInt(pos++, queueAgencyId);
            stmt.setString(pos++, worker);
            stmt.setInt(pos++, priority);
            stmt.setInt(pos, selectAgencyId);

            return stmt.executeUpdate();
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new QueueException("Error when queue agency", ex);
        }
    }

    public List<QueueStatDTO> getQueueStatsByWorker() throws QueueException {
        LOGGER.entry();
        List<QueueStatDTO> result = new ArrayList<>();

        try {
            return result = getQueueStats(SELECT_QUEUE_COUNT_BY_WORKER);
        } finally {
            LOGGER.exit(result);
        }
    }

    public List<QueueStatDTO> getQueueStatsByAgency() throws QueueException {
        LOGGER.entry();
        List<QueueStatDTO> result = new ArrayList<>();

        try {
            return result = getQueueStats(SELECT_QUEUE_COUNT_BY_AGENCY);
        } finally {
            LOGGER.exit(result);
        }
    }

    private List<QueueStatDTO> getQueueStats(String queueQuery) throws QueueException {
        LOGGER.entry(queueQuery);
        List<QueueStatDTO> result = new ArrayList<>();

        try (Connection connection = dataSource.getConnection();
             Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(queueQuery)) {
                while (resultSet.next()) {
                    final String text = resultSet.getString("text");
                    final int count = resultSet.getInt("count");
                    final String date = resultSet.getString("max");

                    result.add(new QueueStatDTO(text, count, date));
                }
            }

            return result;
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex.getMessage());
            throw new QueueException("Error when getting queue stats", ex);
        } finally {
            LOGGER.exit(result);
        }
    }

}
