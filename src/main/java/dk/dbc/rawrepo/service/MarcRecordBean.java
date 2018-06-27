package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXChangeMimeType;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import javax.ws.rs.InternalServerErrorException;
import javax.ws.rs.NotFoundException;
import javax.ws.rs.WebApplicationException;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.Charset;
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

    private RawRepoDAO createDAO(Connection conn) throws RawRepoException {
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

    private MarcXMerger getMerger(boolean overwriteCommonAgency) {
        if (overwriteCommonAgency) {
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

    private MarcRecord rawRecordToMarcRecord(Record rawRecord) throws MarcReaderException {
        final byte[] content = rawRecord.getContent();
        final InputStream inputStream = new ByteArrayInputStream(content);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);

        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, Charset.forName("UTF-8"));

        return reader.read();
    }

    public MarcRecord getRawRecord(String bibliographicRecordId, int agencyId) throws WebApplicationException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                final Record rawRecord = dao.fetchRecord(bibliographicRecordId, agencyId);

                if (rawRecord.getContent() == null || rawRecord.getContent().length == 0) {
                    throw new NotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
                }

                final MarcRecord result = rawRecordToMarcRecord(rawRecord);

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            } catch (MarcReaderException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerErrorException(ex.getMessage(), ex);
        }
    }

    public MarcRecord getRecordMergedOrExpanded(String bibliographicRecordId, int agencyId,
                                                boolean doExpand, boolean allowDeleted, boolean excludeDBCFields,
                                                boolean overwriteCommonAgency) throws WebApplicationException {
        try (Connection conn = globalDataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);
                Record rawRecord;

                if (doExpand) {
                    rawRecord = dao.fetchMergedRecordExpanded(bibliographicRecordId, agencyId, getMerger(overwriteCommonAgency), allowDeleted);
                } else {
                    rawRecord = dao.fetchMergedRecord(bibliographicRecordId, agencyId, getMerger(overwriteCommonAgency), allowDeleted);
                }

                if (rawRecord.getContent() == null || rawRecord.getContent().length == 0) {
                    throw new NotFoundException("Posten '" + bibliographicRecordId + ":" + Integer.toString(agencyId) + "' blev ikke fundet");
                }

                MarcRecord result = rawRecordToMarcRecord(rawRecord);

                if (excludeDBCFields) {
                    result = removePrivateFields(result);
                }

                return result;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            } catch (MarcReaderException | MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerErrorException(ex.getMessage(), ex);
        }
    }

    public Collection<MarcRecord> getRecordCollection(String bibliographicRecordId, int agencyId,
                                                      boolean allowDeleted, boolean excludeDBCFields,
                                                      boolean overwriteCommonAgency) throws WebApplicationException {
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
                    collection = dao.fetchRecordCollectionExpanded(bibliographicRecordId, agencyId, getMerger(overwriteCommonAgency));
                }

                final Collection<MarcRecord> marcRecords = new HashSet<>();
                for (Map.Entry<String, Record> entry : collection.entrySet()) {
                    final Record rawRecord = entry.getValue();
                    if (!isMarcXChange(rawRecord.getMimeType())) {
                        throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
                    }

                    MarcRecord record = rawRecordToMarcRecord(rawRecord);

                    if (excludeDBCFields) {
                        record = removePrivateFields(record);
                    }

                    marcRecords.add(record);
                }

                return marcRecords;
            } catch (RawRepoException ex) {
                conn.rollback();
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            } catch (MarcReaderException | MarcXMergerException ex) {
                LOGGER.error(ex.getMessage(), ex);
                throw new InternalServerErrorException(ex.getMessage(), ex);
            }
        } catch (SQLException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerErrorException(ex.getMessage(), ex);
        }
    }

}
