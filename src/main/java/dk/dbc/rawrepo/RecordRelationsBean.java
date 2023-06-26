package dk.dbc.rawrepo;

import dk.dbc.common.records.ExpandCommonMarcRecord;
import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.exception.RecordServiceRuntimeException;
import dk.dbc.rawrepo.service.RecordObjectMapper;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.Stateless;
import javax.inject.Inject;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Stateless
public class RecordRelationsBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordRelationsBean.class);

    RelationHintsVipCore relationHints;

    private static final List<String> AUTHORITY_FIELDS = ExpandCommonMarcRecord.AUTHORITY_FIELD_LIST;

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @Inject
    VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;

    @Inject
    RecordSimpleBean recordSimpleBean;

    @Inject
    RawRepoBean rawRepoBean;

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
            relationHints = new RelationHintsVipCore(vipCoreLibraryRulesConnector);
        } catch (Exception ex) {
            throw new RecordServiceRuntimeException(ex);
        }
    }

    public int findParentRelationAgency(String bibliographicRecordId, int originalAgencyId) throws RecordNotFoundException, RawRepoException, VipCoreException {
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

                // There is never a parent relation for DBC enrichments, so we might as well just skip those
                if (agencyId != RecordBeanUtils.DBC_ENRICHMENT_AGENCY) {
                    final Record record = recordSimpleBean.fetchRecord(bibliographicRecordId, agencyId);
                    final MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());
                    final boolean usesEnrichments = vipCoreLibraryRulesConnector.hasFeature(agencyId, VipCoreLibraryRulesConnector.Rule.USE_ENRICHMENTS);

                    for (DataField dataField : marcRecord.getFields(DataField.class)) {

                        // head/section/volume structure
                        if ("014".equals(dataField.getTag())) {
                            String valueA = null;
                            String valueX = null;

                            for (SubField subField : dataField.getSubFields()) {
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

                            for (SubField subField : dataField.getSubFields()) {
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
                        // If there are authority references in the record but the agencies isn't using
                        // enrichments and thereby not using authority records just skip the authority fields
                        if (usesEnrichments && AUTHORITY_FIELDS.contains(dataField.getTag())) {
                            String value5 = null;
                            String value6 = null;

                            for (SubField subField : dataField.getSubFields()) {
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
        } catch (SQLException | RawRepoException | MarcReaderException | VipCoreException ex) {
            LOGGER.error(ex.getMessage(), ex);
            throw new InternalServerException(ex.getMessage(), ex);
        }
    }

    /**
     * This function traverses the parents and if there is a record of the given agency, and that record is active then
     * true is returned, otherwise false.
     * <p>
     * In short this function is used to determine if a deleted volume for an agency should be included in the DataIO
     * collection or not
     *
     * @param bibliographicRecordId ID of the record to find the records for
     * @param agencyId              The original agency
     * @return True if there is an active parent for the given agency, otherwise false
     * @throws RecordNotFoundException If the record isn't found
     * @throws InternalServerException If there is an unhandled exception
     * @throws RawRepoException        If rawrepo throws an exception
     */
    @SuppressWarnings("PMD")
    public boolean parentIsActive(String bibliographicRecordId,
                                  int agencyId) throws RecordNotFoundException, InternalServerException, RawRepoException, VipCoreException {
        // There are no relations on enrichments other than to the common records. So in order to find the parent section
        // or head record we have to look at the common volume's parents
        final int mostCommonAgency = findParentRelationAgency(bibliographicRecordId, agencyId);
        final Set<RecordId> parents = getRelationsParents(bibliographicRecordId, mostCommonAgency);
        for (RecordId parent : parents) {
            // The parent will be a common record. In order to see if there is an enrichment for the original agency
            // we have to look at all records with that bibliographic record id
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

    public Map<RecordId, Set<RecordId>> getRelationsChildren(Set<RecordId> recordIds) throws InternalServerException {
        return getRelations(recordIds, RelationsType.CHILDREN);
    }

    public Map<RecordId, Set<RecordId>> getRelationsSiblingsToMe(Set<RecordId> recordIds) throws InternalServerException {
        return getRelations(recordIds, RelationsType.SIBLINGS_TO_ME);
    }

    private Map<RecordId, Set<RecordId>> getRelations(Set<RecordId> recordIds, RelationsType mode) throws InternalServerException {
        final Map<RecordId, Set<RecordId>> result = new HashMap<>();
        try {
            // There is a limit on how many values there can be in a query which means we have to slice up the input
            // The exact limit is not known but 1000 entries seems to be a reasonable amount
            int index = 0;
            final int sliceSize = 1000;
            while (index < recordIds.size()) {
                LOGGER.info("Index: {}", index);
                final Set<RecordId> sliceSet = recordIds.stream()
                        .skip(index)
                        .limit(sliceSize)
                        .collect(Collectors.toSet());

                index += sliceSize;

                final Map<RecordId, Set<RecordId>> slice = rawRepoBean.getRelations(sliceSet, mode);

                result.putAll(slice);
            }

            return result;
        } catch (RawRepoException e) {
            throw new InternalServerException(e.getMessage(), e);
        }
    }

    public Set<RecordId> getRelationsSiblingsFromMe(String bibliographicRecordId, int agencyId) throws
            InternalServerException, RawRepoException, RecordNotFoundException, VipCoreException {
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

                if (RecordBeanUtils.DBC_AGENCIES.contains(agencyId)) {
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
