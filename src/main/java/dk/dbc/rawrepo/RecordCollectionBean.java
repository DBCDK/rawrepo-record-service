/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
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
import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

    @EJB
    RecordSimpleBean recordSimpleBean;

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
                        throw new MarcXMergerException(
                                String.format("Cannot make marcx:collection for record %s:%s with mimetype '%s'",
                                        rawRecord.getId().getBibliographicRecordId(),
                                        rawRecord.getId().getAgencyId(),
                                        rawRecord.getMimeType()));
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
                                                         boolean useParentAgency,
                                                         boolean expand,
                                                         boolean handleControlRecords) throws InternalServerException, RecordNotFoundException {
        final Map<String, Record> collection = new HashMap<>();
        final Map<String, Record> result = new HashMap<>();
        try (Connection conn = dataSource.getConnection()) {
            try {
                fetchDataIORecordCollection(collection, bibliographicRecordId, originalAgencyId, useParentAgency, expand, true, false, handleControlRecords);

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
            } catch (MarcXMergerException | MarcReaderException e) {
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
                                             boolean useParentAgency,
                                             boolean expand,
                                             boolean isRoot,
                                             boolean allowDeletedParent,
                                             boolean handleControlRecords) throws RecordNotFoundException, InternalServerException, RawRepoException, MarcReaderException {
        if (!collection.containsKey(bibliographicRecordId)) {
            boolean newAllowDeletedParent = allowDeletedParent;
            Record record;
            if (isRoot) {
                // In most cases we want the deleted volume enrichment. However in the situation where there is an active
                // section or head enrichment for the FBS agency and a deleted volume record the collection should instead
                // include the active common volume and the active head/section enrichment
                if (recordRelationsBean.parentIsActive(bibliographicRecordId, agencyId)) {
                    record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, useParentAgency, expand, isLocalRecord(bibliographicRecordId));
                } else {
                    record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, useParentAgency, expand, true);
                }

                // Root record is of the correct agencyId
                newAllowDeletedParent = RecordBeanUtils.DBC_AGENCIES.contains(agencyId) || record.getId().getAgencyId() != agencyId;
            } else {
                try {
                    // Assume the record an enrichment
                    record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, useParentAgency, expand, allowDeletedParent);
                } catch (RecordNotFoundException e) {
                    // If no active record is found try to find deleted record
                    if (!allowDeletedParent) {
                        record = recordBean.getDataIORawRepoRecord(bibliographicRecordId, agencyId, useParentAgency, expand, true);
                    } else {
                        // If the first attempt was made with allow deleted then we have no hope of finding the record
                        // so just throw the exception
                        throw e;
                    }
                }
            }
            collection.put(bibliographicRecordId, record);

            final int mostCommonAgency = recordRelationsBean.findParentRelationAgency(bibliographicRecordId, agencyId);
            final Set<RecordId> parents = recordRelationsBean.getRelationsParents(bibliographicRecordId, mostCommonAgency);

            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId) {
                    continue;
                }
                fetchDataIORecordCollection(collection, parent.getBibliographicRecordId(), agencyId, useParentAgency, expand, false, newAllowDeletedParent, handleControlRecords);
            }


            /*
                For certain dataio jobs (at the time of coding daily proof printing) it is necessary to include
                referenced control records. The control records are referenced in subfields 520 *n and 526 *n.
                Because there are no "real" relations to these records we have to look in the record content to find
                them. We must include the full hierarchy for the control records if it is a volume record.
             */
            if (handleControlRecords) {
                final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(new ByteArrayInputStream(record.getContent()), StandardCharsets.UTF_8);
                final MarcRecord marcRecord = reader.read();
                final List<String> values520n = new ArrayList<>();
                values520n.addAll(marcRecord.getSubFieldValues("520", 'n'));
                values520n.addAll(marcRecord.getSubFieldValues("526", 'n'));

                for (String value : values520n) {
                    if (recordSimpleBean.recordExists(value, agencyId, false)) {
                        fetchDataIORecordCollection(collection, value, agencyId, useParentAgency, expand, false, newAllowDeletedParent, handleControlRecords);
                    }
                }
            }
        }
    }

    private boolean isLocalRecord(String bibliographicRecordId) throws InternalServerException {
        Set<Integer> agencies = recordRelationsBean.getAllAgenciesForBibliographicRecordId(bibliographicRecordId);

        return !agencies.contains(870970);
    }

}
