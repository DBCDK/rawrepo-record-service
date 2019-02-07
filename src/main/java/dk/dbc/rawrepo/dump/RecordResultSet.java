/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class RecordResultSet implements AutoCloseable {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(DumpService.class);

    private Connection connection;
    private PreparedStatement preparedStatement;
    private ResultSet resultSet;
    private Params params;
    private int agencyId;
    private AgencyType agencyType;

    public RecordResultSet(Connection connection, Params params, int agencyId, AgencyType agencyType, int fetchSize) throws SQLException {
        this.connection = connection;
        this.params = params;
        this.agencyId = agencyId;
        this.agencyType = agencyType;

        // Since the request could result in a very large dataset we want to use a cursor to open the result set
        // This is done by setAutoCommit(false) and setFetchSize(fetchSize)
        connection.setAutoCommit(false);

        prepare();

        preparedStatement.setFetchSize(fetchSize);

        resultSet = preparedStatement.executeQuery();
    }

    private void prepare() throws SQLException {
        LOGGER.info("Prepare agency type: {}", agencyType);
        if (agencyType == AgencyType.DBC) {
            prepareDBC();
        } else if (agencyType == AgencyType.FBS) {
            prepareFBS();
        } else {
            prepareLocal();
        }
    }

    private void prepareDBC() throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append(prepareSQLEnrichment());

        if (params.getRowLimit() > 0) {
            sb.append(prepareSQLLimit());
        }

        preparedStatement = connection.prepareStatement(sb.toString());

        int pos = 1;

        pos = prepareStatementEnrichment(pos, agencyId, 191919);

        if (params.getRowLimit() > 0) {
            pos = prepareStatementLimit(pos, params.getRowLimit());
        }
    }

    private void prepareFBS() throws SQLException {
        StringBuilder sb = new StringBuilder();

        if (this.params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
            sb.append(prepareSQLEnrichment());
        }

        if (this.params.getRecordType().contains(RecordType.LOCAL.toString()) &&
                this.params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
            sb.append(" UNION ");
        }

        if (this.params.getRecordType().contains(RecordType.LOCAL.toString())) {
            sb.append(prepareSQLLocal());
        }

        if (params.getRowLimit() > 0) {
            sb.append(prepareSQLLimit());
        }

        preparedStatement = connection.prepareStatement(sb.toString());

        int pos = 1;

        if (this.params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
            pos = prepareStatementEnrichment(pos, 870970, agencyId);
        }

        if (this.params.getRecordType().contains(RecordType.LOCAL.toString())) {
            pos = prepareStatementLocal(pos, agencyId);
        }

        if (params.getRowLimit() > 0) {
            pos = prepareStatementLimit(pos, params.getRowLimit());
        }
    }

    private void prepareLocal() throws SQLException {
        StringBuilder sb = new StringBuilder();

        sb.append(prepareSQLLocal());

        if (params.getRowLimit() > 0) {
            sb.append(prepareSQLLimit());
        }

        preparedStatement = connection.prepareStatement(sb.toString());

        int pos = 1;

        pos = prepareStatementLocal(pos, agencyId);

        if (params.getRowLimit() > 0) {
            pos = prepareStatementLimit(pos, params.getRowLimit());
        }
    }

    private String prepareSQLEnrichment() {
        final StringBuilder sb = new StringBuilder();
        sb.append("SELECT common.bibliographicrecordid, convert_from(decode(common.content, 'base64'), 'UTF-8'),");
        sb.append(" convert_from(decode(local.content, 'base64'), 'UTF-8')");
        sb.append(" FROM records as common, records as local");
        sb.append(" WHERE common.agencyid=?");
        sb.append(" AND local.agencyid=?");
        sb.append(" AND common.bibliographicrecordid = local.bibliographicrecordid");

        final RecordStatus recordStatus = RecordStatus.fromString(params.getRecordStatus());

        if (recordStatus == RecordStatus.DELETED) {
            sb.append(" AND common.deleted = 't'");
        }

        if (recordStatus == RecordStatus.ACTIVE) {
            sb.append(" AND common.deleted = 'f'");
        }

        if (params.getCreated() != null) {
            if (params.getCreated().getFrom() != null) {
                sb.append(" AND common.created > ?");
            }

            if (params.getCreated().getTo() != null) {
                sb.append(" AND common.created < ?");
            }
        }

        if (params.getModified() != null) {
            if (params.getModified().getFrom() != null) {
                sb.append(" AND common.modified > ?");
            }

            if (params.getModified().getTo() != null) {
                sb.append(" AND common.modified < ?");
            }
        }

        return sb.toString();
    }

    private int prepareStatementEnrichment(int pos, int commonAgencyId, int localAgencyId) throws SQLException {
        preparedStatement.setInt(pos++, commonAgencyId);
        preparedStatement.setInt(pos++, localAgencyId);

        if (params.getCreated() != null) {
            if (params.getCreated().getFrom() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getCreated().getFrom()));
            }

            if (params.getCreated().getTo() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getCreated().getTo()));
            }
        }

        if (params.getModified() != null) {
            if (params.getModified().getFrom() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getModified().getFrom()));
            }

            if (params.getModified().getTo() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getModified().getTo()));
            }
        }

        return pos;
    }

    private String prepareSQLLocal() {
        final StringBuilder sb = new StringBuilder();

        sb.append("SELECT local.bibliographicrecordid, '', convert_from(decode(local.content, 'base64'), 'UTF-8')");
        sb.append(" FROM records as local");
        sb.append(" WHERE local.agencyid=?");

        final RecordStatus recordStatus = RecordStatus.fromString(params.getRecordStatus());

        if (recordStatus == RecordStatus.ACTIVE) {
            sb.append(" AND local.deleted = 'f'");
        } else if (recordStatus == RecordStatus.DELETED) {
            sb.append(" AND local.deleted = 't'");
        }
        // If recordStatus == RecordStatus.ALL then we just ignore the deleted column

        if (params.getCreated() != null) {
            if (params.getCreated().getFrom() != null) {
                sb.append(" AND local.created > ?").append(params.getCreated().getFrom());
            }

            if (params.getCreated().getTo() != null) {
                sb.append(" AND local.created < ?").append(params.getCreated().getTo());
            }
        }

        if (params.getModified() != null) {
            if (params.getModified().getFrom() != null) {
                sb.append(" AND local.modified > ?").append(params.getModified().getFrom());
            }

            if (params.getModified().getTo() != null) {
                sb.append(" AND local.modified < ?").append(params.getModified().getTo());
            }
        }

        return sb.toString();
    }

    private int prepareStatementLocal(int pos, int localAgencyId) throws SQLException {
        preparedStatement.setInt(pos++, localAgencyId);

        if (params.getCreated() != null) {
            if (params.getCreated().getFrom() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getCreated().getFrom()));
            }

            if (params.getCreated().getTo() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getCreated().getTo()));
            }
        }

        if (params.getModified() != null) {
            if (params.getModified().getFrom() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getModified().getFrom()));
            }

            if (params.getModified().getTo() != null) {
                preparedStatement.setDate(pos++, Date.valueOf(params.getModified().getTo()));
            }
        }

        return pos;
    }

    private String prepareSQLLimit() {
        return " LIMIT ?";
    }

    private int prepareStatementLimit(int pos, int limit) throws SQLException {
        preparedStatement.setInt(pos++, limit);

        return pos;
    }

    public RecordItem next() throws SQLException {
        synchronized (this) {
            if (resultSet.next()) {
                return new RecordItem(resultSet.getString(1), resultSet.getBytes(2), resultSet.getBytes(3));
            } else {
                return null;
            }
        }
    }

    @Override
    public void close() {
        synchronized (this) {
            try {
                if (!preparedStatement.isClosed()) {
                    this.preparedStatement.close();
                }
            } catch (SQLException e) {
                LOGGER.error("Exception when closing PreparedStatement", e);
            }
            try {
                if (!connection.isClosed()) {
                    this.connection.close();
                }
            } catch (SQLException e) {
                LOGGER.error("Exception when closing Connection", e);
            }
        }
    }

}
