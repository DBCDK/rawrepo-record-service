/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
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

    private MarcXMerger defaultMerger;
    private MarcXMerger overwriteMerger;
    private RelationHintsOpenAgency relationHints;

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
                return true;
            default:
                return false;
        }
    }

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        try {
            RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
            rawRepoBuilder.relationHints(relationHints);
            return rawRepoBuilder.build();
        } finally {
            LOGGER.info("rawrepo.createDAO");
        }
    }

    @PostConstruct
    public void init() {
        try {
            defaultMerger = new MarcXMerger();

            final String immutable = "001;010;020;990;991;996";
            final String overwrite = "004;005;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

            final FieldRules customFieldRules = new FieldRules(immutable, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);

            overwriteMerger = new MarcXMerger(customFieldRules, "RECORD_SERVICE");

            relationHints = new RelationHintsOpenAgency(openAgency.getService());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    private MarcXMerger getMerger(boolean useParentAgency) {
        if (useParentAgency) {
            return overwriteMerger;
        } else {
            return defaultMerger;
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

    @Timed
    public boolean recordExists(String bibliographicRecordId, int agencyId, boolean maybeDeleted) throws InternalServerException {
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
                throw new InternalServerException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
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

                MarcXMerger merger = getMerger(useParentAgency);

                boolean recordExists = dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId);
                boolean isDeleted = recordExists && !dao.recordExists(bibliographicRecordId, agencyId);

                // If the record doesn't exist at all or if the record is marked as deleted and the result should not
                // include deleted records then return null.
                if (!recordExists || isDeleted && !allowDeleted, keepAutFields) {
                    return null;
                } else if (isDeleted) {
                    // There are no relations on deleted records to we have to handle merging 191919 records in a different way
                    if (useParentAgency && agencyId == 191919) {
                        rawRecord = mergeDeletedRecord(bibliographicRecordId, agencyId, merger, dao);
                    } else {
                        rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);
                    }
                } else {
                    if (doExpand) {
                        rawRecord = dao.fetchMergedRecordExpanded(bibliographicRecordId, agencyId, merger, allowDeleted);
                    } else {
                        rawRecord = dao.fetchMergedRecord(bibliographicRecordId, agencyId, merger, allowDeleted);
                    }
                }

                MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                if (excludeDBCFields) {
                    marcRecord = removePrivateFields(marcRecord);
                }

                rawRecord.setContent(RecordObjectMapper.marcToContent(marcRecord));

                return rawRecord;
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
        final List<Integer> possibleParentAgencies = Arrays.asList(870970, 870971, 870979);

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
     * @param agencyId              Agency of the child/enrichment record
     * @param merger                Merge object
     * @param dao                   Reference to the RawRepoAccess DAP
     * @return Record object with merged values from the input record and its parent record
     * @throws RawRepoException
     * @throws RecordNotFoundException
     * @throws MarcXMergerException
     */
    Record mergeDeletedRecord(String bibliographicRecordId, int agencyId, MarcXMerger merger, RawRepoDAO dao) throws RawRepoException, RecordNotFoundException, MarcXMergerException, MarcReaderException {
        int parentAgency = parentCommonAgencyId(bibliographicRecordId, dao);

        final Record deletedCommon = dao.fetchRecord(bibliographicRecordId, parentAgency);
        final Record deletedEnrichment = dao.fetchRecord(bibliographicRecordId, agencyId);

        byte[] content = merger.merge(deletedCommon.getContent(), deletedEnrichment.getContent(), true);
        StringBuilder enrichmentTrail = new StringBuilder(deletedCommon.getEnrichmentTrail());
        enrichmentTrail.append(',').append(deletedEnrichment.getId().getAgencyId());

        return RecordImpl.enriched(bibliographicRecordId, deletedEnrichment.getId().getAgencyId(), true,
                merger.mergedMimetype(deletedCommon.getMimeType(), deletedEnrichment.getMimeType()), content,
                deletedCommon.getCreated().isAfter(deletedEnrichment.getCreated()) ? deletedCommon.getCreated() : deletedEnrichment.getCreated(),
                deletedCommon.getModified().isAfter(deletedEnrichment.getModified()) ? deletedCommon.getModified() : deletedEnrichment.getModified(),
                deletedCommon.getModified().isAfter(deletedEnrichment.getModified()) ? deletedCommon.getTrackingId() : deletedEnrichment.getTrackingId(),
                enrichmentTrail.toString());
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

                if (allowDeleted &&
                        !dao.recordExists(bibliographicRecordId, agencyId) &&
                        dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                    final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);
                    collection = new HashMap<>();
                    collection.put(bibliographicRecordId, rawRecord);
                } else {
                    if (expand) {
                        collection = dao.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, getMerger(useParentAgency),true, keepAutFields);
                    } else {
                        collection = dao.fetchRecordCollection(bibliographicRecordId, agencyId, getMerger(useParentAgency));
                    }
                }

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
    public Map<String, Record> getRawRepoRecordCollection(String bibliographicRecordId, int agencyId,
                                                          boolean allowDeleted, boolean excludeDBCFields,
                                                          boolean useParentAgency,
                                                          boolean expand, boolean keepAutFields) throws InternalServerException {
        Map<String, Record> collection;

        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                if (allowDeleted &&
                        !dao.recordExists(bibliographicRecordId, agencyId) &&
                        dao.recordExistsMaybeDeleted(bibliographicRecordId, agencyId)) {
                    final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);
                    collection = new HashMap<>();
                    collection.put(bibliographicRecordId, rawRecord);
                } else {
                    collection = dao.fetchRecordCollection(bibliographicRecordId, agencyId, getMerger(useParentAgency));
                }


                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    if (expand) {
                        dao.expandRecord(rawRecord, keepAutFields);
                    }

                    MarcRecord record = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                    if (excludeDBCFields) {
                        rawRecord.setContent(RecordObjectMapper.marcToContent(removePrivateFields(record)));
                    }
                }

                return collection;
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
        Set<RecordId> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                result = dao.getRelationsParents(recordId);

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

    public Set<RecordId> getRelationsSiblingsFromMe(String bibliographicRecordId, int agencyId) throws InternalServerException {
        Set<RecordId> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                result = dao.getRelationsSiblingsFromMe(recordId);

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

    public Set<RecordId> getRelationsSiblingsToMe(String bibliographicRecordId, int agencyId) throws InternalServerException {
        Set<RecordId> result;
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

                result = dao.getRelationsSiblingsToMe(recordId);

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

}
