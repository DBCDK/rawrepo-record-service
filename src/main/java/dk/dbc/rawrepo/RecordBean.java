/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcrecord.ExpandCommonMarcRecord;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import dk.dbc.rawrepo.pool.ObjectPool;
import dk.dbc.rawrepo.service.RecordObjectMapper;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static dk.dbc.rawrepo.RecordBeanUtils.removePrivateFields;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class RecordBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    private OpenAgencyBean openAgency;

    @EJB
    RecordSimpleBean recordSimpleBean;

    @EJB
    RecordRelationsBean recordRelationsBean;

    private final ObjectPool<MarcXMerger> customMarcXMergerPool = new CustomMarcXMergerPool();
    private final ObjectPool<MarcXMerger> defaultMarcXMergerPool = new DefaultMarcXMergerPool();

    RelationHintsOpenAgency relationHints;

    // Constructor used for mocking
    RecordBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    public RecordBean() {

    }

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

    private ObjectPool<MarcXMerger> getMergerPool(boolean useParentAgency) {
        if (useParentAgency) {
            return customMarcXMergerPool;
        } else {
            return defaultMarcXMergerPool;
        }
    }

    @Timed
    public Record getRawRepoRecordRaw(String bibliographicRecordId, int agencyId, boolean allowDeleted) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                if (allowDeleted) {
                    if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + agencyId + "' blev ikke fundet");
                    }
                } else {
                    if (!dao.recordExists(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + agencyId + "' blev ikke fundet eller er slettet");
                    }
                }

                return dao.fetchRecord(bibliographicRecordId, agencyId);
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

    @Timed
    public Record getRawRepoRecordMerged(String bibliographicRecordId,
                                         int agencyId,
                                         boolean allowDeleted,
                                         boolean excludeDBCFields,
                                         boolean useParentAgency) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecord(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, false, false);
    }

    @Timed
    public Record getRawRepoRecordExpanded(String bibliographicRecordId,
                                           int agencyId,
                                           boolean allowDeleted,
                                           boolean excludeDBCFields,
                                           boolean useParentAgency,
                                           boolean keepAutFields) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecord(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, true, keepAutFields);
    }

    private Record getRawRepoRecord(String bibliographicRecordId,
                                    int agencyId,
                                    boolean allowDeleted,
                                    boolean excludeDBCFields,
                                    boolean useParentAgency,
                                    boolean doExpand,
                                    boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                final Record rawRecord;
                final ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                final MarcXMerger merger = mergePool.checkOut();
                final int correctedAgencyId = dao.agencyFor(bibliographicRecordId, agencyId, allowDeleted);

                rawRecord = fetchMostRelevantRecord(bibliographicRecordId, correctedAgencyId, allowDeleted, merger, doExpand, keepAutFields);

                mergePool.checkIn(merger);

                if (excludeDBCFields) {
                    MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());
                    marcRecord = removePrivateFields(marcRecord);
                    rawRecord.setContent(RecordObjectMapper.marcToContent(marcRecord));
                }

                return rawRecord;
            } catch (RawRepoExceptionRecordNotFound ex) {
                return null;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            } catch (MarcReaderException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    /**
     * Since there are no relations for deleted records we have to look at what other agencies exists for the deleted
     * record and try to match it to a DBC agency.
     *
     * @param bibliographicRecordId Id of the record
     * @param dao                   Reference to the RawRepoAccess DAP
     * @return The parent DBC agency
     * @throws RawRepoException
     * @throws RecordNotFoundException
     */
    int parentCommonAgencyId(String bibliographicRecordId, RawRepoDAO dao) throws RawRepoException, RecordNotFoundException {
        final Set<Integer> agencies = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);
        final List<Integer> possibleParentAgencies = Arrays.asList(870970, 870971, 870974, 870979, 190002, 190004);

        for (Integer agency : agencies) {
            if (possibleParentAgencies.contains(agency)) {
                return agency;
            }
        }

        throw new RecordNotFoundException("Parent agency for record with bibliographicRecordId " + bibliographicRecordId + " could not be found");
    }

    /**
     * This function returns a merged deleted record.
     * <p>
     * Deleted records don't have relations so we have to get the parent and child record and then merge them together
     * unlike just fetching the merged record.
     * <p>
     * Note: this function is only intended for 191919 records but might work with other agencies
     *
     * @param bibliographicRecordId Id of the record
     * @param originalAgencyId      Agency of the child/enrichment record
     * @param merger                Merge object
     * @return Record object with merged values from the input record and its parent record
     * @throws RawRepoException
     * @throws RecordNotFoundException
     * @throws InternalServerException
     */
    private Record fetchMostRelevantRecord(String bibliographicRecordId, int originalAgencyId, boolean allowDeleted, MarcXMerger merger, boolean doExpand, boolean keepAutField) throws InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            final RawRepoDAO dao = createDAO(conn);
            int agencyId;

            try {
                if (!allowDeleted || recordSimpleBean.recordIsActive(bibliographicRecordId, originalAgencyId)) {
                    agencyId = dao.agencyFor(bibliographicRecordId, originalAgencyId, allowDeleted);
                } else {
                    try {
                        agencyId = dao.agencyFor(bibliographicRecordId, originalAgencyId, false);
                    } catch (RawRepoExceptionRecordNotFound e) {
                        if (allowDeleted) {
                            agencyId = dao.agencyFor(bibliographicRecordId, originalAgencyId, true);
                        } else {
                            throw e;
                        }
                    }
                }
            } catch (RawRepoExceptionRecordNotFound e) {
                // Recast exception to local type in order to catch that specific exception
                // Otherwise RawRepoExceptionRecordNotFound will be caught as a generic RawRepoException somewhere
                throw new RecordNotFoundException(e.getMessage());
            }

            if (recordSimpleBean.recordIsActive(bibliographicRecordId, agencyId)) {
                if (doExpand) {
                    return dao.fetchMergedRecordExpanded(bibliographicRecordId, agencyId, merger, false, keepAutField);
                } else {
                    return dao.fetchMergedRecord(bibliographicRecordId, agencyId, merger, false);
                }
            } else {
                final LinkedList<Record> records = new LinkedList<>();

                for (; ; ) {
                    final Record record = recordSimpleBean.fetchRecord(bibliographicRecordId, agencyId);
                    records.addFirst(record);

                    final Set<RecordId> siblings = recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, agencyId);

                    if (siblings.isEmpty()) {
                        break;
                    }
                    agencyId = siblings.iterator().next().getAgencyId();
                }
                final Iterator<Record> iterator = records.iterator();
                Record record = iterator.next();
                if (iterator.hasNext()) { // Record will be merged
                    byte[] content = record.getContent();
                    final StringBuilder enrichmentTrail = new StringBuilder(record.getEnrichmentTrail());

                    while (iterator.hasNext()) {
                        final Record next = iterator.next();
                        if (!merger.canMerge(record.getMimeType(), next.getMimeType())) {
                            LOGGER.error("Cannot merge: " + record.getMimeType() + " and " + next.getMimeType());
                            throw new MarcXMergerException("Cannot merge enrichment");
                        }

                        content = merger.merge(content, next.getContent(), next.getId().getAgencyId() == originalAgencyId);
                        enrichmentTrail.append(',').append(next.getId().getAgencyId());

                        record = RecordImpl.fromCache(bibliographicRecordId, next.getId().getAgencyId(), true,
                                merger.mergedMimetype(record.getMimeType(), next.getMimeType()), content,
                                record.getCreated().isAfter(next.getCreated()) ? record.getCreated() : next.getCreated(),
                                record.getModified().isAfter(next.getModified()) ? record.getModified() : next.getModified(),
                                record.getModified().isAfter(next.getModified()) ? record.getTrackingId() : next.getTrackingId(),
                                enrichmentTrail.toString());
                    }
                }

                if (doExpand) {
                    expandRecord(record, keepAutField);
                }

                return record;
            }
        } catch (SQLException | MarcXMergerException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }

    void expandRecord(Record record, boolean keepAutField) throws
            RawRepoException, RecordNotFoundException, InternalServerException {
        final RecordId recordId = record.getId();
        final String bibliographicRecordId = recordId.getBibliographicRecordId();
        final int agencyId = recordId.getAgencyId();

        try (Connection conn = dataSource.getConnection()) {
            if (recordSimpleBean.recordIsActive(bibliographicRecordId, agencyId)) {
                final RawRepoDAO dao = createDAO(conn);

                dao.expandRecord(record, keepAutField);
            } else {
                RecordId expandableRecordId = null;

                // Only these agencies can have authority parents
                final List<Integer> expandableAgencies = Arrays.asList(190002, 190004, 870970, 870971, 870974);

                if (expandableAgencies.contains(recordId.agencyId)) {
                    expandableRecordId = recordId;
                } else {
                    final Set<RecordId> relationsSiblings = recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, agencyId);
                    for (int expandableAgencyId : expandableAgencies) {
                        final RecordId potentialExpandableRecordId = new RecordId(bibliographicRecordId, expandableAgencyId);
                        if (relationsSiblings.contains(potentialExpandableRecordId)) {
                            expandableRecordId = potentialExpandableRecordId;
                            break;
                        }
                    }
                }

                if (expandableRecordId != null) {
                    final Set<RecordId> autParents = recordRelationsBean.getRelationsParents(expandableRecordId.bibliographicRecordId, expandableRecordId.agencyId);
                    final Map<String, Record> autRecords = new HashMap<>();

                    for (RecordId parentId : autParents) {
                        if ("870979".equals(Integer.toString(parentId.getAgencyId()))) {
                            LOGGER.info("expandRecord - E");
                            autRecords.put(parentId.getBibliographicRecordId(), recordSimpleBean.fetchRecord(parentId.getBibliographicRecordId(), parentId.getAgencyId()));
                        }
                    }

                    ExpandCommonMarcRecord.expandRecord(record, autRecords, keepAutField);
                }
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }


}
