/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dump.Params;
import dk.dbc.rawrepo.dump.RecordItem;
import dk.dbc.rawrepo.dump.RecordStatus;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class RawRepoBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoBean.class);

    private static final String QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY = "SELECT bibliographicrecordid FROM records WHERE agencyid=? AND deleted='f'";
    private static final String QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY_ALL = "SELECT bibliographicrecordid FROM records WHERE agencyid=?";
    private static final String QUERY_AGENCIES = "SELECT DISTINCT(agencyid) FROM records";
    private static final String SET_SERVER_URL_CONFIGURATION = "INSERT INTO configurations (key, value) VALUES (?, ?) ON CONFLICT (key) DO NOTHING";

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @Timed
    public List<String> getBibliographicRecordIdForAgency(int agencyId, boolean allowDeleted) throws RawRepoException {
        try {
            ArrayList<String> ret = new ArrayList<>();

            String query = allowDeleted ? QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY_ALL : QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY;

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                stmt.setInt(1, agencyId);
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        String bibliographicRecordId = resultSet.getString(1);
                        ret.add(bibliographicRecordId);
                    }
                }
            }

            return ret;
        } catch (SQLException ex) {
            throw new RawRepoException("Error getting bibliographicrecordids", ex);
        }
    }

    @Timed
    public List<String> getBibliographicRecordIdForAgencyInterval(int agencyId, boolean allowDeleted, String createdBefore, String createdAfter, String modifiedBefore, String modifiedAfter) throws RawRepoException {
        try {
            ArrayList<String> ret = new ArrayList<>();

            String query = allowDeleted ? QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY_ALL : QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY;

            if (hasValue(createdBefore)) {
                query += " AND created < ?";
            }

            if (hasValue(createdAfter)) {
                query += " AND created >= ?";
            }

            if (hasValue(modifiedBefore)) {
                query += " AND modified < ?";
            }

            if (hasValue(modifiedAfter)) {
                query += " AND modified >= ?";
            }

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(query)) {
                int i = 0;
                stmt.setInt(++i, agencyId);
                if (hasValue(createdBefore))
                    stmt.setDate(++i, Date.valueOf(createdBefore));
                if (hasValue(createdAfter))
                    stmt.setDate(++i, Date.valueOf(createdAfter));
                if (hasValue(modifiedBefore))
                    stmt.setDate(++i, Date.valueOf(modifiedBefore));
                if (hasValue(modifiedAfter))
                    stmt.setDate(++i, Date.valueOf(modifiedAfter));
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        String bibliographicRecordId = resultSet.getString(1);
                        ret.add(bibliographicRecordId);
                    }
                }
            }

            return ret;
        } catch (SQLException ex) {
            throw new RawRepoException("Error getting bibliographicrecordids", ex);
        }
    }

    @Timed
    public List<Integer> getAgencies() throws RawRepoException {
        try {
            ArrayList<Integer> ret = new ArrayList<>();

            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(QUERY_AGENCIES)) {
                try (ResultSet resultSet = stmt.executeQuery()) {
                    while (resultSet.next()) {
                        int agencyId = resultSet.getInt(1);
                        ret.add(agencyId);
                    }
                }
            }

            return ret;
        } catch (SQLException ex) {
            throw new RawRepoException("Error getting agencies", ex);
        }
    }

    public void setConfigurations(String key, String value) throws RawRepoException {
        try {
            try (Connection connection = dataSource.getConnection();
                 PreparedStatement stmt = connection.prepareStatement(SET_SERVER_URL_CONFIGURATION)) {
                stmt.setString(1, key);
                stmt.setString(2, value);
                stmt.execute();
            }
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex);
            throw new RawRepoException("Error updating configurations", ex);
        }
    }

    public List<RecordItem> getDecodedContent(List<String> bibliographicRecordIds, Integer commonAgencyId, Integer localAgencyId, Params params) throws RawRepoException {
        List<RecordItem> res = new ArrayList<>();
        List<String> placeHolders = new ArrayList<>();
        for (int i = 0; i < bibliographicRecordIds.size(); i++) {
            placeHolders.add("?");
        }

        String query;

        // Local record
        if (commonAgencyId == null) {
            query = " SELECT local.bibliographicrecordid, " +
                    "        null, " +
                    "        convert_from(decode(local.content, 'base64'), 'UTF-8')" +
                    "   FROM records as local" +
                    "  WHERE local.agencyid=?";
        } else { // Enrichment record
            query = " SELECT common.bibliographicrecordid, " +
                    "        convert_from(decode(common.content, 'base64'), 'UTF-8')," +
                    "        convert_from(decode(local.content, 'base64'), 'UTF-8')" +
                    "   FROM records as common, records as local" +
                    "  WHERE common.agencyid=?" +
                    "    AND local.agencyid=?" +
                    "    AND common.bibliographicrecordid = local.bibliographicrecordid";
        }

        query += "       AND local.bibliographicrecordid in (" + String.join(",", placeHolders) + ")";

        RecordStatus recordStatus = RecordStatus.fromString(params.getRecordStatus());

        if (recordStatus == RecordStatus.DELETED) {
            query += "   AND common.deleted = 't'";
        }

        if (recordStatus == RecordStatus.ACTIVE) {
            query += "   AND common.deleted = 'f'";
        }

        if (params.getCreatedFrom() != null) {
            query += "   AND local.created < ? ::timestamp AT TIME ZONE 'CET'";
        }

        if (params.getCreatedTo() != null) {
            query += "   AND local.created >= ? ::timestamp AT TIME ZONE 'CET'";
        }

        if (params.getModifiedFrom() != null) {
            query += "   AND local.modified < ? ::timestamp AT TIME ZONE 'CET'";
        }

        if (params.getModifiedTo() != null) {
            query += "   AND local.modified >= ? ::timestamp AT TIME ZONE 'CET'";
        }

        int pos = 1;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            if (commonAgencyId != null) {
                preparedStatement.setInt(pos++, commonAgencyId);
            }
            preparedStatement.setInt(pos++, localAgencyId);

            for (String bibliographicRecordId : bibliographicRecordIds) {
                preparedStatement.setString(pos++, bibliographicRecordId);
            }

            if (params.getCreatedFrom() != null) {
                preparedStatement.setTimestamp(pos++, Timestamp.valueOf(params.getCreatedFrom()));
            }

            if (params.getCreatedTo() != null) {
                preparedStatement.setTimestamp(pos++, Timestamp.valueOf(params.getCreatedTo()));
            }

            if (params.getModifiedFrom() != null) {
                preparedStatement.setTimestamp(pos++, Timestamp.valueOf(params.getModifiedFrom()));
            }

            if (params.getModifiedTo() != null) {
                preparedStatement.setTimestamp(pos++, Timestamp.valueOf(params.getModifiedFrom()));
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                res.add(new RecordItem(resultSet.getString(1), resultSet.getBytes(2), resultSet.getBytes(3)));
            }
        } catch (SQLException ex) {
            LOGGER.info("Caught exception: {}", ex);
            throw new RawRepoException("Error during getBibliographicRecordIdsForEnrichmentAgency", ex);
        }

        return res;
    }

    public HashMap<String, String> getMimeTypeForRecordIds(List<String> bibliographicRecordIds, int agencyId) throws SQLException {
        HashMap<String, String> res = new HashMap<>();

        List<String> placeHolders = new ArrayList<>();
        for (int i = 0; i < bibliographicRecordIds.size(); i++) {
            placeHolders.add("?");
        }

        String query = "SELECT bibliographicrecordid, mimetype FROM records ";
        query += "       WHERE agencyid=? ";
        query += "         AND bibliographicrecordid in (" + String.join(",", placeHolders) + ")";

        int pos = 1;

        try (Connection connection = dataSource.getConnection();
             PreparedStatement preparedStatement = connection.prepareStatement(query)) {
            preparedStatement.setInt(pos++, agencyId);

            for (String bibliographicRecordId : bibliographicRecordIds) {
                preparedStatement.setString(pos++, bibliographicRecordId);
            }

            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                res.put(resultSet.getString(1), resultSet.getString(2));
            }
        }

        return res;
    }

    private boolean hasValue(String s) {
        return !(s == null || s.isEmpty());
    }

}
