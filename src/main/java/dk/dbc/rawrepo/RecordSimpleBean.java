/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import dk.dbc.rawrepo.pool.ObjectPool;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;

@Stateless
public class RecordSimpleBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordSimpleBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    private OpenAgencyBean openAgency;

    private final ObjectPool<MarcXMerger> customMarcXMergerPool = new CustomMarcXMergerPool();
    private final ObjectPool<MarcXMerger> defaultMarcXMergerPool = new DefaultMarcXMergerPool();

    RelationHintsOpenAgency relationHints;

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        final RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
        rawRepoBuilder.relationHints(relationHints);
        return rawRepoBuilder.build();
    }

    // Constructor used for mocking
    RecordSimpleBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    public RecordSimpleBean() {

    }

    private ObjectPool<MarcXMerger> getMergerPool(boolean useParentAgency) {
        if (useParentAgency) {
            return customMarcXMergerPool;
        } else {
            return defaultMarcXMergerPool;
        }
    }

    @PostConstruct
    public void init() {
        try {
            relationHints = new RelationHintsOpenAgency(openAgency.getService());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public boolean recordIsActive(String bibliographicRecordId, int agencyId) throws RawRepoException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            final RawRepoDAO dao = createDAO(conn);

            if (dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                return dao.recordExists(bibliographicRecordId, agencyId);
            } else {
                throw new RecordNotFoundException(String.format("Record %s:%s doesn't exist", bibliographicRecordId, agencyId));
            }

        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }

    @Timed
    public boolean recordExists(String bibliographicRecordId, int agencyId, boolean maybeDeleted) throws RawRepoException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                if (maybeDeleted) {
                    return dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId);
                } else {
                    return dao.recordExists(bibliographicRecordId, agencyId);
                }
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new RawRepoException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }

    public Record fetchRecord(String bibliographicRecordId, int agencyId) throws InternalServerException, RecordNotFoundException {
        final Record result;
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.fetchRecord(bibliographicRecordId, agencyId);

                // fetchRecord will return a new empty record if the requested record is not found in the database.
                // One solution would be to check if the record exists but that requires an extra request to the database
                // In order to save a round trip we just check if the record has any content.
                if (result.getContent().length == 0) {
                    throw new RecordNotFoundException(String.format("The Record %s:%s does not exist", bibliographicRecordId, agencyId));
                } else {
                    return result;
                }
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

    public Record fetchRecordMerged(String bibliographicRecordId, int agencyId, boolean allowAll, boolean useParentAgency) throws InternalServerException, RecordNotFoundException {
        Record result;
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                final ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                final MarcXMerger merger = mergePool.checkOut();
                result = dao.fetchMergedRecord(bibliographicRecordId, agencyId, merger, allowAll);
                mergePool.checkIn(merger);

                return result;
            } catch (RawRepoExceptionRecordNotFound ex) {
                throw new RecordNotFoundException(String.format("The Record %s:%s does not exist", bibliographicRecordId, agencyId));
            } catch (RawRepoException | MarcXMergerException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Record fetchRecordExpanded(String bibliographicRecordId, int agencyId, boolean allowAll, boolean useParentAgency) throws InternalServerException, RecordNotFoundException {
        Record result;
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                final ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                final MarcXMerger merger = mergePool.checkOut();
                result = dao.fetchMergedRecordExpanded(bibliographicRecordId, agencyId, merger, allowAll);
                mergePool.checkIn(merger);

                return result;
            } catch (RawRepoExceptionRecordNotFound ex) {
                throw new RecordNotFoundException(String.format("The Record %s:%s does not exist", bibliographicRecordId, agencyId));
            } catch (RawRepoException | MarcXMergerException ex) {
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
