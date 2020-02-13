package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.service.RecordObjectMapper;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.sql.DataSource;
import java.sql.Connection;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static dk.dbc.marcxmerge.MarcXChangeMimeType.isMarcXChange;
import static dk.dbc.rawrepo.RecordBeanUtils.removePrivateFields;

@Stateless
public class MarcRecordBean {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordSimpleBean.class);

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @EJB
    private OpenAgencyBean openAgency;

    @EJB
    RecordBean recordBean;

    @EJB
    RecordCollectionBean recordCollectionBean;

    RelationHintsOpenAgency relationHints;

    // Constructor used for mocking
    MarcRecordBean(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    // Default constructor - required as there is another constructor
    public MarcRecordBean() {

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

    @Timed
    public MarcRecord getMarcRecordMerged(String bibliographicRecordId, int agencyId,
                                          boolean allowDeleted, boolean excludeDBCFields,
                                          boolean useParentAgency) throws InternalServerException, RecordNotFoundException {
        try {
            final Record record = recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency);

            if (record == null) {
                return null;
            } else {
                return RecordObjectMapper.contentToMarcRecord(record.getContent());
            }
        } catch (MarcReaderException e) {
            throw new InternalServerException();
        }
    }

    @Timed
    public MarcRecord getMarcRecordExpanded(String bibliographicRecordId, int agencyId,
                                            boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency,
                                            boolean keepAutFields) throws InternalServerException, RecordNotFoundException {
        try {
            final Record record = recordBean.getRawRepoRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);

            if (record == null) {
                return null;
            } else {
                return RecordObjectMapper.contentToMarcRecord(record.getContent());
            }
        } catch (MarcReaderException e) {
            throw new InternalServerException();
        }
    }

    @Timed
    public Collection<MarcRecord> getMarcRecordCollection(String bibliographicRecordId, int agencyId,
                                                          boolean allowDeleted,
                                                          boolean excludeDBCFields,
                                                          boolean useParentAgency,
                                                          boolean expand,
                                                          boolean keepAutFields,
                                                          boolean excludeAutRecords) throws InternalServerException, RecordNotFoundException, MarcXMergerException, MarcReaderException {
        final Map<String, Record> collection = recordCollectionBean.getRawRepoRecordCollection(bibliographicRecordId,
                agencyId,
                allowDeleted,
                excludeDBCFields,
                useParentAgency,
                expand,
                keepAutFields,
                excludeAutRecords);

        final Collection<MarcRecord> marcRecords = new HashSet<>();

        for (Map.Entry<String, Record> entry : collection.entrySet()) {
            final Record rawRecord = entry.getValue();
            if (!isMarcXChange(rawRecord.getMimeType())) {
                throw new MarcXMergerException("Cannot make marcx:collection from mimetype: " + rawRecord.getMimeType());
            }

            MarcRecord record = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());

            marcRecords.add(record);
        }

        return marcRecords;
    }
}
