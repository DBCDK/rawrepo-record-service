/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

@Stateless
public class MarcRecordBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(MarcRecordBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource globalDataSource;

    @EJB
    private OpenAgencyBean openAgency;

    private MarcXMerger defaultMerger;
    private MarcXMerger overwriteMerger;

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
            rawRepoBuilder.relationHints(new RelationHintsOpenAgency(openAgency.getService()));
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

            overwriteMerger = new MarcXMerger(customFieldRules);
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

    public Record getRawRepoRecordMerged(String bibliographicRecordId, int agencyId,
                                         boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, false, false);
    }

    public Record getRawRepoRecordExpanded(String bibliographicRecordId, int agencyId, boolean allowDeleted, boolean excludeDBCFields,
                                           boolean useParentAgency, boolean keepAutFields) throws RecordNotFoundException, InternalServerException {
        return getRawRepoRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, true, keepAutFields);
    }

    private Record getRawRepoRecordMergedOrExpanded(String bibliographicRecordId, int agencyId,
                                                    boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                                    boolean doExpand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                Record rawRecord;

                rawRecord = dao.fetchMergedRecord(bibliographicRecordId, agencyId, getMerger(useParentAgency), allowDeleted);

                if (doExpand) {
                    dao.expandRecord(rawRecord, keepAutFields);
                }

                if (rawRecord.getContent() == null || rawRecord.getContent().length == 0) {
                    throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
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

    public MarcRecord getMarcRecordMerged(String bibliographicRecordId, int agencyId,
                                          boolean allowDeleted, boolean excludeDBCFields,
                                          boolean useParentAgency) throws InternalServerException, RecordNotFoundException {
        return getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, false, false);
    }

    public MarcRecord getMarcRecordExpanded(String bibliographicRecordId, int agencyId,
                                            boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                            boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        return getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, true, keepAutFields);
    }

    private MarcRecord getMarcRecordMergedOrExpanded(String bibliographicRecordId, int agencyId,
                                                     boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                                     boolean doExpand, boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                Record rawRecord;

                rawRecord = dao.fetchMergedRecord(bibliographicRecordId, agencyId, getMerger(useParentAgency), allowDeleted);

                if (rawRecord.getContent() == null || rawRecord.getContent().length == 0) {
                    throw new RecordNotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
                }

                if (doExpand) {
                    dao.expandRecord(rawRecord, keepAutFields);
                }

                MarcRecord result = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

                if (excludeDBCFields) {
                    result = removePrivateFields(result);
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
                    collection = dao.fetchRecordCollection(bibliographicRecordId, agencyId, getMerger(useParentAgency));
                }

                final Collection<MarcRecord> marcRecords = new HashSet<>();
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

}
