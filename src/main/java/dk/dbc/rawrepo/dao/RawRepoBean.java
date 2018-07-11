/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dao;

import dk.dbc.rawrepo.RawRepoException;
import org.perf4j.StopWatch;
import org.perf4j.log4j.Log4JStopWatch;

import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

@Stateless
public class RawRepoBean {
    // Outcommented because PMD is angry about unused variable but we will probably need it in the future
    //private static final XLogger LOGGER = XLoggerFactory.getXLogger(RawRepoBean.class);

    private static final String QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY = "SELECT bibliographicrecordid FROM records where agencyid=? and deleted='f'";
    private static final String QUERY_BIBLIOGRAPHICRECORDID_BY_AGENCY_ALL = "SELECT bibliographicrecordid FROM records where agencyid=?";
    private static final String QUERY_AGENCIES = "SELECT DISTINCT(agencyid) FROM records";

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    public List<String> getBibliographicRecordIdForAgency(int agencyId, boolean allowDeleted) throws RawRepoException {
        StopWatch watch = new Log4JStopWatch();
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
        } finally {
            watch.stop("RawRepoBean.getBibliographicRecordIdForAgency");
        }
    }

    public List<Integer> getAgencies() throws RawRepoException {
        StopWatch watch = new Log4JStopWatch();
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
        } finally {
            watch.stop("RawRepoBean.getAgencies");
        }
    }

}
