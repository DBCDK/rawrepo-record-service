/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.dao.OpenAgencyBean;
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
import java.sql.SQLException;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class QueueBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueBean.class);

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

    @Timed
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
}
