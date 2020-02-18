package dk.dbc.rawrepo;

import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.service.RecordObjectMapper;
import dk.dbc.util.Timed;

import javax.annotation.PostConstruct;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;

import static dk.dbc.marcxmerge.MarcXChangeMimeType.isMarcXChange;

@Stateless
public class MarcRecordBean {

    @EJB
    private OpenAgencyBean openAgency;

    @EJB
    RecordBean recordBean;

    @EJB
    RecordCollectionBean recordCollectionBean;

    RelationHintsOpenAgency relationHints;

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
    public Collection<MarcRecord> getMarcRecordCollection(String bibliographicRecordId,
                                                          int agencyId,
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

        return recordsToMarcRecords(collection);
    }

    @Timed
    public Collection<MarcRecord> getDataIOMarcRecordCollection(String bibliographicRecordId,
                                                                int agencyId,
                                                                boolean expand) throws InternalServerException, RecordNotFoundException, MarcXMergerException, MarcReaderException {
        final Map<String, Record> collection = recordCollectionBean.getDataIORecordCollection(bibliographicRecordId,
                agencyId,
                expand);

        return recordsToMarcRecords(collection);
    }

    @NotNull
    private Collection<MarcRecord> recordsToMarcRecords(Map<String, Record> collection) throws MarcXMergerException, MarcReaderException {
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
