/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

@Interceptors(StopwatchInterceptor.class)
@Stateless
public class MarcRecordBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(MarcRecordBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource globalDataSource;

    @EJB
    private OpenAgencyBean openAgency;

    private ObjectPool<MarcXMerger> customMarcXMergerPool = new CustomMarcXMergerPool();
    private ObjectPool<MarcXMerger> defaultMarcXMergerPool = new DefaultMarcXMergerPool();

    RelationHintsOpenAgency relationHints;

    private final Integer DBC_ENRICHMENT = 191919;
    private final List<String> AUTHORITY_FIELDS = Arrays.asList("100", "600", "700", "770", "780");

    // Constructor used for mocking
    MarcRecordBean(DataSource globalDataSource) {
        this.globalDataSource = globalDataSource;
    }

    // Default constructor - required as there is another constructor
    public MarcRecordBean() {

    }

    private static boolean isMarcXChange(String mimeType) {
        switch (mimeType) {
            case MarcXChangeMimeType.AUTHORITY:
            case MarcXChangeMimeType.ARTICLE:
            case MarcXChangeMimeType.ENRICHMENT:
            case MarcXChangeMimeType.MARCXCHANGE:
            case MarcXChangeMimeType.LITANALYSIS:
                return true;
            default:
                return false;
        }
    }

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
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

    private MarcRecord removePrivateFields(MarcRecord marcRecord) throws MarcReaderException {
        final List<Field> fields = new ArrayList<>();

        for (Field field : marcRecord.getFields()) {
            if (field.getTag().matches("^[0-9].*")) {
                fields.add(field);
            }
        }

        final MarcRecord newMarcRecord = new MarcRecord();
        newMarcRecord.setLeader(marcRecord.getLeader());
        newMarcRecord.addAllFields(fields);

        return newMarcRecord;
    }

    // TODO Add test cases
    Map<String, Record> fetchRecordCollection(String bibliographicRecordId, int agencyId, MarcXMerger merger) throws RawRepoException, InternalServerException, MarcXMergerException, RecordNotFoundException {
        LOGGER.info("fetchRecordCollection 1 for {}:{}", bibliographicRecordId, agencyId);
        if (recordIsActive(bibliographicRecordId, agencyId)) {
            try (Connection conn = globalDataSource.getConnection()) {
                final RawRepoDAO dao = createDAO(conn);

                return dao.fetchRecordCollection(bibliographicRecordId, agencyId, merger);
            } catch (SQLException | MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new RawRepoException(ex.getMessage(), ex);
            }
        } else {
            HashMap<String, Record> ret = new HashMap<>();

            fetchRecordCollection(ret, bibliographicRecordId, agencyId, merger, true);

            return ret;
        }
    }

    // TODO Add test cases
    private void fetchRecordCollection(Map<String, Record> collection, String bibliographicRecordId, int agencyId, MarcXMerger merger, boolean includeAut) throws RawRepoException, InternalServerException, MarcXMergerException, RecordNotFoundException {
        if (!collection.containsKey(bibliographicRecordId)) {
            final Record record = fetchMergedRecord(bibliographicRecordId, agencyId, merger);

            collection.put(bibliographicRecordId, record);

            final int mostCommonAgency = findParentRelationAgency(bibliographicRecordId, agencyId);
            final Set<RecordId> parents = getRelationsParents(bibliographicRecordId, mostCommonAgency);
            for (RecordId parent : parents) {
                // If this parent is an authority record and includeAut is false then skip parent
                if (870979 == parent.agencyId && !includeAut) {
                    continue;
                }
                fetchRecordCollection(collection, parent.getBibliographicRecordId(), agencyId, merger, includeAut);
            }
        }
    }

    int findParentRelationAgency(String bibliographicRecordId, int originalAgencyId) throws RawRepoException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            if (recordIsActive(bibliographicRecordId, originalAgencyId)) {
                final RawRepoDAO dao = createDAO(conn);

                return dao.findParentRelationAgency(bibliographicRecordId, originalAgencyId);
            } else {
                if (relationHints.usesCommonAgency(originalAgencyId)) {
                    final List<Integer> list = relationHints.get(originalAgencyId);
                    if (!list.isEmpty()) {
                        for (Integer agencyId : list) {
                            if (recordExists(bibliographicRecordId, agencyId, true)) {
                                return agencyId;
                            }
                        }
                    }
                }
                if (recordExists(bibliographicRecordId, originalAgencyId, true)) {
                    return originalAgencyId;
                }
            }
        } catch (SQLException ex) {
            throw new RawRepoException(ex.getMessage(), ex);
        }
        throw new RawRepoExceptionRecordNotFound("Could not find (parent) relation agency for " + bibliographicRecordId + " from " + originalAgencyId);
    }

    boolean recordIsActive(String bibliographicRecordId, int agencyId) throws RawRepoException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            final RawRepoDAO dao = createDAO(conn);

            if (dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                return dao.recordExists(bibliographicRecordId, agencyId);
            } else {
                throw new RecordNotFoundException("Record doesn't exist");
            }

        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }

    @Timed
    public boolean recordExists(String bibliographicRecordId, int agencyId, boolean maybeDeleted) throws RawRepoException {
        try (Connection conn = globalDataSource.getConnection()) {
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

    @Timed
    public Record getRawRepoRecordRaw(String bibliographicRecordId, int agencyId, boolean allowDeleted) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                if (allowDeleted) {
                    if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
                    }
                } else {
                    if (!dao.recordExists(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet eller er slettet");
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
    public Record getRawRepoRecordMerged(String bibliographicRecordId, int agencyId,
                                         boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, false, false);
    }

    @Timed
    public Record getRawRepoRecordExpanded(String bibliographicRecordId, int agencyId, boolean allowDeleted, boolean excludeDBCFields,
                                           boolean useParentAgency, boolean keepAutFields) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, true, keepAutFields);
    }

    private Record getRawRepoRecordMergedOrExpanded(String bibliographicRecordId, int agencyId,
                                                    boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                                    boolean doExpand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try {
            Record rawRecord = getRawRepoRecord(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, doExpand, keepAutFields);

            if (rawRecord == null) {
                return null;
            }

            MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

            if (excludeDBCFields) {
                marcRecord = removePrivateFields(marcRecord);
            }

            rawRecord.setContent(RecordObjectMapper.marcToContent(marcRecord));

            return rawRecord;
        } catch (MarcReaderException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    // keepAutFields will be used later
    @SuppressWarnings("PMD.UnusedFormalParameter")
    private Record getRawRepoRecord(String bibliographicRecordId, int agencyId,
                                    boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                    boolean doExpand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                Record rawRecord;

                ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                MarcXMerger merger = mergePool.checkOut();

                int correctedAgencyId = dao.agencyFor(bibliographicRecordId, agencyId, allowDeleted);

                boolean recordExists = dao.recordExistsMaybeDeleted(bibliographicRecordId, correctedAgencyId);
                boolean isDeleted = recordExists && !dao.recordExists(bibliographicRecordId, correctedAgencyId);

                // If the record doesn't exist at all or if the record is marked as deleted and the result should not
                // include deleted records then return null.
                if (!recordExists || isDeleted && !allowDeleted) {
                    return null;
                } else if (isDeleted) {
                    // There are no relations on deleted records to we have to handle merging 191919 records in a different way
                    rawRecord = fetchMergedRecord(bibliographicRecordId, correctedAgencyId, merger);
                    //TODO expand
                } else {
                    if (doExpand) {
                        rawRecord = dao.fetchMergedRecordExpanded(bibliographicRecordId, correctedAgencyId, merger, allowDeleted);
                    } else {
                        rawRecord = dao.fetchMergedRecord(bibliographicRecordId, correctedAgencyId, merger, allowDeleted);
                    }
                }

                mergePool.checkIn(merger);

                MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                if (excludeDBCFields) {
                    marcRecord = removePrivateFields(marcRecord);
                }

                rawRecord.setContent(RecordObjectMapper.marcToContent(marcRecord));

                return rawRecord;
            } catch (RawRepoExceptionRecordNotFound ex) {
                return null;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            } catch (MarcReaderException | MarcXMergerException ex) {
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
     * @throws MarcXMergerException
     */
    Record fetchMergedRecord(String bibliographicRecordId, int originalAgencyId, MarcXMerger merger) throws MarcXMergerException, InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            if (recordIsActive(bibliographicRecordId, originalAgencyId)) {
                final RawRepoDAO dao = createDAO(conn);

                return dao.fetchMergedRecord(bibliographicRecordId, originalAgencyId, merger, false);
            } else {
                final RawRepoDAO dao = createDAO(conn);

                int agencyId = dao.agencyFor(bibliographicRecordId, originalAgencyId, true);
                final LinkedList<Record> records = new LinkedList<>();
                for (; ; ) {
                    final Record record = fetchRecord(bibliographicRecordId, agencyId);
                    records.addFirst(record);

                    final Set<RecordId> siblings = getRelationsSiblingsFromMe(bibliographicRecordId, agencyId);
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

                return record;
            }
        } catch (SQLException | MarcXMergerException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new RawRepoException(ex.getMessage(), ex);
        }
    }

    @Timed
    public MarcRecord getMarcRecord(String bibliographicRecordId, int agencyId, boolean allowDeleted, boolean excludeDBCFields) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                if (allowDeleted) {
                    if (!dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
                    }
                } else {
                    if (!dao.recordExists(bibliographicRecordId, agencyId)) {
                        throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet eller er slettet");
                    }
                }

                final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);

                MarcRecord result = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                if (excludeDBCFields) {
                    result = removePrivateFields(result);
                }

                return result;
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

    @Timed
    public MarcRecord getMarcRecordMerged(String bibliographicRecordId, int agencyId,
                                          boolean allowDeleted, boolean excludeDBCFields,
                                          boolean useParentAgency) throws InternalServerException, RecordNotFoundException {
        return getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, false, false);
    }

    @Timed
    public MarcRecord getMarcRecordExpanded(String bibliographicRecordId, int agencyId,
                                            boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                            boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        return getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, true, keepAutFields);
    }

    private MarcRecord getMarcRecordMergedOrExpanded(String bibliographicRecordId, int agencyId,
                                                     boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                                     boolean doExpand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try {
            Record rawRecord = getRawRepoRecord(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, doExpand, keepAutFields);

            if (rawRecord == null) {
                return null;
            }

            MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

            if (excludeDBCFields) {
                marcRecord = removePrivateFields(marcRecord);
            }

            return marcRecord;
        } catch (MarcReaderException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    @Timed
    public Collection<MarcRecord> getMarcRecordCollection(String bibliographicRecordId, int agencyId,
                                                          boolean allowDeleted, boolean excludeDBCFields,
                                                          boolean useParentAgency,
                                                          boolean expand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        Map<String, Record> collection;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                MarcXMerger merger = mergePool.checkOut();

                if (!dao.recordExists(bibliographicRecordId, agencyId) &&
                        dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                    final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);
                    collection = new HashMap<>();
                    collection.put(bibliographicRecordId, rawRecord);
                } else {
                    if (expand) {
                        collection = dao.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, merger, true, keepAutFields);
                    } else {
                        collection = dao.fetchRecordCollection(bibliographicRecordId, agencyId, merger);
                    }
                }

                mergePool.checkIn(merger);

                final Collection<MarcRecord> marcRecords = new HashSet<>();
                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    MarcRecord record = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                    if (excludeDBCFields) {
                        record = removePrivateFields(record);
                    }

                    marcRecords.add(record);
                }

                return marcRecords;
            } catch (RawRepoExceptionRecordNotFound ex) {
                LOGGER.error("Record not found", ex);
                throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + agencyId + "' blev ikke fundet");
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            } catch (MarcReaderException | MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    @Timed
    public Map<String, Record> getRawRepoRecordCollection(String bibliographicRecordId,
                                                          int agencyId,
                                                          boolean allowDeleted,
                                                          boolean excludeDBCFields,
                                                          boolean useParentAgency,
                                                          boolean expand,
                                                          boolean keepAutFields,
                                                          boolean excludeAutRecords) throws InternalServerException {
        Map<String, Record> collection;
        Map<String, Record> result = new HashMap<>();

        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                ObjectPool<MarcXMerger> mergePool = getMergerPool(useParentAgency);
                MarcXMerger merger = mergePool.checkOut();

                if (allowDeleted &&
                        !dao.recordExists(bibliographicRecordId, agencyId) &&
                        dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                    final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);
                    collection = new HashMap<>();
                    collection.put(bibliographicRecordId, rawRecord);
                } else {
                    if (expand) {
                        collection = dao.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, merger, true, keepAutFields);
                    } else {
                        collection = dao.fetchRecordCollection(bibliographicRecordId, agencyId, merger);
                    }
                }

                mergePool.checkIn(merger);

                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    // excludeAutRecords indicate authority records should be thrown away unless it is the requested record
                    if (excludeAutRecords && MarcXChangeMimeType.AUTHORITY.equals(rawRecord.getMimeType()) && !bibliographicRecordId.equals(rawRecord.getId().getBibliographicRecordId())) {
                        continue;
                    }

                    MarcRecord record = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                    if (excludeDBCFields) {
                        rawRecord.setContent(RecordObjectMapper.marcToContent(removePrivateFields(record)));
                    }

                    result.put(entry.getKey(), rawRecord);
                }

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            } catch (MarcReaderException | MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Set<RecordId> getRelationsParents(String bibliographicRecordId, int agencyId) throws InternalServerException {
        try (Connection conn = globalDataSource.getConnection()) {
            if (recordIsActive(bibliographicRecordId, agencyId)) {
                try {
                    final RawRepoDAO dao = createDAO(conn);

                    final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                    return dao.getRelationsParents(recordId);
                } catch (RawRepoException ex) {
                    conn.rollback();
                    LOGGER.error(ex.getMessage(), ex);
                    throw new InternalServerException(ex.getMessage(), ex);
                }
            } else {
                final Set<RecordId> result = new HashSet<>();

                // There is never a parent relations for DBC enrichments so we might as well just skip those
                if (agencyId != DBC_ENRICHMENT) {
                    MarcRecord record = getMarcRecord(bibliographicRecordId, agencyId, true, false);

                    for (Field field : record.getFields()) {
                        final DataField dataField = (DataField) field;

                        // head/section/volume structure
                        if ("014".equals(dataField.getTag())) {
                            String valueA = null;
                            String valueX = null;

                            for (SubField subField : dataField.getSubfields()) {
                                if ('a' == subField.getCode()) {
                                    valueA = subField.getData();
                                } else if ('x' == subField.getCode()) {
                                    valueX = subField.getData();
                                }
                            }

                            if (valueA != null) {
                                if (valueX == null || "DEB".equals(valueX)) {
                                    result.add(new RecordId(valueA, agencyId));
                                } else if ("ANM".equals(valueX)) {
                                    result.add(new RecordId(valueA, 870970));
                                }
                            }
                        }

                        // Handling for littolk records
                        if (870974 == agencyId && ("016".equals(dataField.getTag()) || "018".equals(dataField.getTag()))) {
                            String valueA = null;
                            String value5 = null;

                            for (SubField subField : dataField.getSubfields()) {
                                if ('a' == subField.getCode()) {
                                    valueA = subField.getData();
                                } else if ('5' == subField.getCode()) {
                                    value5 = subField.getData();
                                }
                            }

                            if (valueA != null) {
                                if (value5 != null) {
                                    result.add(new RecordId(valueA, Integer.parseInt(value5)));
                                } else {
                                    result.add(new RecordId(valueA, agencyId));
                                }
                            }
                        }

                        // Handle authority fields
                        if (AUTHORITY_FIELDS.contains(dataField.getTag())) {
                            String value5 = null;
                            String value6 = null;

                            for (SubField subField : dataField.getSubfields()) {
                                if ('5' == subField.getCode()) {
                                    value5 = subField.getData();
                                } else if ('6' == subField.getCode()) {
                                    value6 = subField.getData();
                                }
                            }

                            if (value5 != null && value6 != null) {
                                result.add(new RecordId(value6, Integer.parseInt(value5)));
                            }
                        }
                    }
                }

                return result;
            }
        } catch (SQLException | RawRepoException | RecordNotFoundException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }


    public Set<Integer> getAllAgenciesForBibliographicRecordId(String bibliographicRecordId) throws InternalServerException {
        Set<Integer> result;

        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);

                return result;
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

    public Set<RecordId> getRelationsChildren(String bibliographicRecordId, int agencyId) throws InternalServerException {
        Set<RecordId> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                result = dao.getRelationsChildren(recordId);

                return result;
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

    public Set<RecordId> getRelationsSiblingsFromMe(String bibliographicRecordId, int agencyId) throws InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            if (recordIsActive(bibliographicRecordId, agencyId)) {
                try {
                    final RawRepoDAO dao = createDAO(conn);

                    final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                    return dao.getRelationsSiblingsFromMe(recordId);
                } catch (RawRepoException ex) {
                    conn.rollback();
                    LOGGER.error(ex.getMessage(), ex);
                    throw new InternalServerException(ex.getMessage(), ex);
                }
            } else {
                final Set<RecordId> result = new HashSet<>();
                final RawRepoDAO dao = createDAO(conn);
                final List<Integer> potentialSiblingsFromMeAgencies = relationHints.getAgencyPriority(agencyId);
                final Set<Integer> agenciesForRecord = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);

                for (Integer potentialSiblingsFromMeAgency : potentialSiblingsFromMeAgencies) {
                    if (!potentialSiblingsFromMeAgency.equals(agencyId) && agenciesForRecord.contains(potentialSiblingsFromMeAgency)) {
                        result.add(new RecordId(bibliographicRecordId, potentialSiblingsFromMeAgency));
                    }
                }

                return result;
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Set<RecordId> getRelationsSiblingsToMe(String bibliographicRecordId, int agencyId) throws InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            if (recordIsActive(bibliographicRecordId, agencyId)) {
                try {
                    final RawRepoDAO dao = createDAO(conn);

                    final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                    return dao.getRelationsSiblingsToMe(recordId);
                } catch (RawRepoException ex) {
                    conn.rollback();
                    LOGGER.error(ex.getMessage(), ex);
                    throw new InternalServerException(ex.getMessage(), ex);
                }
            } else {
                final Set<RecordId> result = new HashSet<>();
                final RawRepoDAO dao = createDAO(conn);

                if (Arrays.asList(870970, 870971, 870974, 870979, 190002, 190004).contains(agencyId)) {
                    final Set<Integer> agenciesForRecord = dao.allAgenciesForBibliographicRecordId(bibliographicRecordId);

                    for (Integer agencyForRecord : agenciesForRecord) {
                        if (agencyForRecord != agencyId) {
                            result.add(new RecordId(bibliographicRecordId, agencyForRecord));
                        }
                    }
                }

                return result;
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    public Set<RecordId> getRelationsFrom(String bibliographicRecordId, int agencyId) throws InternalServerException {
        Set<RecordId> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                result = dao.getRelationsFrom(recordId);

                return result;
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

    public Record fetchRecord(String bibliographicRecordId, int agencyId) throws InternalServerException {
        Record result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.fetchRecord(bibliographicRecordId, agencyId);

                return result;
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

    public List<RecordMetaDataHistory> getRecordHistory(String bibliographicRecordId, int agencyId) throws InternalServerException {
        List<RecordMetaDataHistory> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.getRecordHistory(bibliographicRecordId, agencyId);

                return result;
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

    public Record getHistoricRecord(RecordMetaDataHistory recordMetaDataHistory) throws InternalServerException {
        Record result;

        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                result = dao.getHistoricRecord(recordMetaDataHistory);

                return result;
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

}
