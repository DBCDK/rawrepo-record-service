package dk.dbc.rawrepo;

import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.service.RecordObjectMapper;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

@Stateless
public class RecordRelationsBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordRelationsBean.class);

    RelationHintsOpenAgency relationHints;

    private final Integer DBC_ENRICHMENT = 191919;
    private final List<String> AUTHORITY_FIELDS = Arrays.asList("100", "600", "700", "770", "780");

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    OpenAgencyBean openAgency;

    @EJB
    RecordSimpleBean recordSimpleBean;

    protected RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        final RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
        rawRepoBuilder.relationHints(relationHints);
        return rawRepoBuilder.build();
    }

    // Constructor used for mocking
    RecordRelationsBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    @SuppressWarnings("unused")
    public RecordRelationsBean() {

    }

    @PostConstruct
    public void init() {
        try {
            relationHints = new RelationHintsOpenAgency(openAgency.getService());
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public int findParentRelationAgency(String bibliographicRecordId, int originalAgencyId) throws RecordNotFoundException, RawRepoException {
        if (relationHints.usesCommonAgency(originalAgencyId)) {
            final List<Integer> list = relationHints.get(originalAgencyId);
            if (!list.isEmpty()) {
                for (Integer agencyId : list) {
                    if (recordSimpleBean.recordExists(bibliographicRecordId, agencyId, true)) {
                        return agencyId;
                    }
                }
            }
        }
        if (recordSimpleBean.recordExists(bibliographicRecordId, originalAgencyId, true)) {
            return originalAgencyId;
        }
        throw new RecordNotFoundException("Could not find (parent) relation agency for " + bibliographicRecordId + " from " + originalAgencyId);
    }

    public Set<RecordId> getRelationsParents(String bibliographicRecordId, int agencyId) throws
            InternalServerException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            if (recordSimpleBean.recordIsActive(bibliographicRecordId, agencyId)) {
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
                    final Record record = recordSimpleBean.fetchRecord(bibliographicRecordId, agencyId);
                    final MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());

                    for (Field field : marcRecord.getFields()) {
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
        } catch (SQLException | RawRepoException | MarcReaderException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    /**
     * This function traverses the parents and if the is a record of the given agency, and that record is active then
     * true is returned, otherwise false.
     * <p>
     * In short this function is used to determine if a deleted volume for an agency should be included in the DataIO
     * collection or not
     *
     * @param bibliographicRecordId Id of the record to find the records for
     * @param agencyId              The original agency
     * @return True if there is an active parent for the given agency, otherwise false
     * @throws RecordNotFoundException If the record isn't found
     * @throws InternalServerException If there is an unhandled exception
     * @throws RawRepoException        If rawrepo throws an exception
     */
    @SuppressWarnings("PMD")
    public boolean parentIsActive(String bibliographicRecordId,
                                  int agencyId) throws RecordNotFoundException, InternalServerException, RawRepoException {
        final int mostCommonAgency = findParentRelationAgency(bibliographicRecordId, agencyId);
        final Set<RecordId> parents = getRelationsParents(bibliographicRecordId, mostCommonAgency);
        for (RecordId parent : parents) {
            Set<Integer> parentAgencies = getAllAgenciesForBibliographicRecordId(parent.getBibliographicRecordId());
            for (int parentAgency : parentAgencies) {
                if (parentAgency == agencyId) {
                    return recordSimpleBean.recordIsActive(parent.getBibliographicRecordId(), parentAgency);
                }
            }
            return parentIsActive(parent.getBibliographicRecordId(), agencyId);
        }

        return false;
    }

    public Set<RecordId> getRelationsChildren(String bibliographicRecordId, int agencyId) throws
            InternalServerException {
        Set<RecordId> result;
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

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

    public Set<RecordId> getRelationsSiblingsFromMe(String bibliographicRecordId, int agencyId) throws
            InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            if (recordSimpleBean.recordIsActive(bibliographicRecordId, agencyId)) {
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

    public Set<RecordId> getRelationsSiblingsToMe(String bibliographicRecordId, int agencyId) throws
            InternalServerException, RawRepoException, RecordNotFoundException {
        try (Connection conn = dataSource.getConnection()) {
            if (recordSimpleBean.recordIsActive(bibliographicRecordId, agencyId)) {
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
        try (Connection conn = dataSource.getConnection()) {
            try {
                final RawRepoDAO dao = createDAO(conn);

                final RecordId recordId = new RecordId(bibliographicRecordId, agencyId);

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

    public Set<Integer> getAllAgenciesForBibliographicRecordId(String bibliographicRecordId) throws
            InternalServerException {
        Set<Integer> result;

        try (Connection conn = dataSource.getConnection()) {
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

}
