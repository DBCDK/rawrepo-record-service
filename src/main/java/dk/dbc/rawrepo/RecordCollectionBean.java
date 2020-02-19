package dk.dbc.rawrepo;

import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import static dk.dbc.marcxmerge.MarcXChangeMimeType.isMarcXChange;

@Stateless
public class RecordCollectionBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordCollectionBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    RecordBean recordBean;

    @EJB
    RecordRelationsBean recordRelationsBean;

    // Constructor used for mocking
    RecordCollectionBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    public RecordCollectionBean() {

    }

    @Timed
    public Map<String, Record> getRawRepoRecordCollection(String bibliographicRecordId,
                                                          int originalAgencyId,
                                                          boolean allowDeleted,
                                                          boolean excludeDBCFields,
                                                          boolean useParentAgency,
                                                          boolean expand,
                                                          boolean keepAutFields,
                                                          boolean excludeAutRecords) throws InternalServerException, RecordNotFoundException {
        final Map<String, Record> collection = new HashMap<>();
        final Map<String, Record> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            try {
                if (expand) {
                    fetchRecordCollectionExpanded(collection, bibliographicRecordId, originalAgencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields, excludeAutRecords);
                } else {
                    fetchRecordCollection(collection, bibliographicRecordId, originalAgencyId, allowDeleted, excludeDBCFields, useParentAgency, excludeAutRecords);
                }

                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    // excludeAutRecords indicate authority records should be thrown away unless it is the requested record
                    if (excludeAutRecords && MarcXChangeMimeType.AUTHORITY.equals(rawRecord.getMimeType()) && !bibliographicRecordId.equals(rawRecord.getId().getBibliographicRecordId())) {
                        continue;
                    }

                    result.put(entry.getKey(), rawRecord);
                }

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            } catch (MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Map<String, Record> getDataIORecordCollection(String bibliographicRecordId,
                                                         int originalAgencyId,
                                                         boolean expand) throws InternalServerException, RecordNotFoundException {
        final Map<String, Record> collection = new HashMap<>();
        final Map<String, Record> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            try {
                fetchDataIORecordCollection(collection, bibliographicRecordId, originalAgencyId, expand, true, false);

                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    // excludeAutRecords indicate authority records should be thrown away unless it is the requested record
                    if (MarcXChangeMimeType.AUTHORITY.equals(rawRecord.getMimeType()) && !bibliographicRecordId.equals(rawRecord.getId().getBibliographicRecordId())) {
                        continue;
                    }

                    result.put(entry.getKey(), rawRecord);
                }

                return result;
            } catch (RawRepoException e) {
                conn.rollback();
                LOGGER.error(e.getMessage(), e);
                throw new InternalServerException(e.getMessage(), e);
            } catch (MarcXMergerException e) {
                LOGGER.error(e.getMessage(), e);
                throw new InternalServerException(e.getMessage(), e);
            }
        } catch (SQLException e) {
            LOGGER.error(e.getMessage(), e);
            throw new InternalServerException(e.getMessage(), e);
        }
    }

    private void fetchRecordCollection(Map<String, Record> collection,
                                       String bibliographicRecordId,
                                       int agencyId,
                                       boolean allowDeleted,
                                       boolean excludeDBCFields,
                                       boolean useParentAgency,
                                       boolean excludeAutRecords) throws RawRepoException, InternalServerException, RecordNotFoundException {
        if (!collection.containsKey(bibliographicRecordId)) {
            final Record record = recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency);
            collection.put(bibliographicRecordId, record);

            final int mostCommonAgency = recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId);
            final Set<RecordId> parents = recordRelationsBean.getRelationsParents(bibliographicRecordId, mostCommonAgency);

            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId && excludeAutRecords) {
                    continue;
                }
                fetchRecordCollection(collection, parent.getBibliographicRecordId(), agencyId, allowDeleted, excludeDBCFields, useParentAgency, excludeAutRecords);
            }
        }
    }

    private void fetchRecordCollectionExpanded(Map<String, Record> collection,
                                               String bibliographicRecordId,
                                               int agencyId,
                                               boolean allowDeleted,
                                               boolean excludeDBCFields,
                                               boolean useParentAgency,
                                               boolean keepAutFields,
                                               boolean excludeAutRecords) throws RawRepoException, InternalServerException, RecordNotFoundException {
        if (!collection.containsKey(bibliographicRecordId)) {
            final Record record = recordBean.getRawRepoRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
            collection.put(bibliographicRecordId, record);

            final int mostCommonAgency = recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId);
            final Set<RecordId> parents = recordRelationsBean.getRelationsParents(bibliographicRecordId, mostCommonAgency);

            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId && excludeAutRecords) {
                    continue;
                }
                fetchRecordCollectionExpanded(collection, parent.getBibliographicRecordId(), agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields, excludeAutRecords);
            }
        }
    }



    private void fetchDataIORecordCollection(Map<String, Record> collection,
                                             String bibliographicRecordId,
                                             int agencyId,
                                             boolean expand,
                                             boolean isRoot,
                                             boolean allowDeletedParent) throws RecordNotFoundException, InternalServerException, RawRepoException {
        if (!collection.containsKey(bibliographicRecordId)) {
            boolean newAllowDeletedParent = allowDeletedParent;
            Record record;
            if (isRoot) {
                if (recordRelationsBean.parentIsActive(bibliographicRecordId, agencyId)) {
                    record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, expand, false);
                } else {
                    record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, expand, true);
                }

                // Root record is of the correct agencyId
                if (record.getId().getAgencyId() == agencyId) {
                    newAllowDeletedParent = false;
                } else {
                    newAllowDeletedParent = true;
                }
            } else {
                record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, expand, allowDeletedParent);
            }
            collection.put(bibliographicRecordId, record);

            final int mostCommonAgency = recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId);
            final Set<RecordId> parents = recordRelationsBean.getRelationsParents(bibliographicRecordId, mostCommonAgency);

            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId) {
                    continue;
                }
                fetchDataIORecordCollection(collection, parent.getBibliographicRecordId(), agencyId, expand, false, newAllowDeletedParent);
            }
        }
    }

}
