package dk.dbc.rawrepo.dto;

import dk.dbc.commons.jsonb.JSONBContext;
import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.rawrepo.output.OutputStreamMarcJsonRecordWriter;
import dk.dbc.rawrepo.service.RecordObjectMapper;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RecordDTOMapper {
    private static final JSONBContext jsonBContext = new JSONBContext();

    public static RecordEntryDTO toRecordEntryDTO(Record record) throws MarcReaderException, MarcWriterException, IOException {
        final RecordEntryDTO dto = new RecordEntryDTO();
        dto.setRecordId(recordIdToDTO(record.getId()));
        dto.setDeleted(record.isDeleted());
        dto.setCreated(record.getCreated().toString());
        dto.setModified(record.getModified().toString());
        dto.setMimetype(record.getMimeType());
        dto.setTrackingId(record.getTrackingId());
        dto.setEnrichmentTrail(record.getEnrichmentTrail());

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final OutputStreamMarcJsonRecordWriter contentWriter =
                new OutputStreamMarcJsonRecordWriter(baos, StandardCharsets.UTF_8.name());
        contentWriter.write(RecordObjectMapper.contentToMarcRecord(record.getContent()));

        dto.setContent(jsonBContext.getObjectMapper().reader().readTree(baos.toByteArray()));

        return dto;
    }

    public static RecordMetaDataDTO recordMetaDataToDTO(Record record) {
        RecordMetaDataDTO dto = new RecordMetaDataDTO();
        dto.setRecordId(recordIdToDTO(record.getId()));
        dto.setDeleted(record.isDeleted());
        dto.setCreated(record.getCreated().toString());
        dto.setModified(record.getModified().toString());
        dto.setMimetype(record.getMimeType());
        dto.setTrackingId(record.getTrackingId());
        dto.setEnrichmentTrail(record.getEnrichmentTrail());

        return dto;
    }

    public static RecordDTO recordToDTO(Record rawRecord, List<String> excludeAttributes) throws MarcReaderException {
        final RecordDTO dto = new RecordDTO();
        dto.setDeleted(rawRecord.isDeleted());
        dto.setCreated(rawRecord.getCreated().toString());
        dto.setModified(rawRecord.getModified().toString());
        dto.setMimetype(rawRecord.getMimeType());
        dto.setTrackingId(rawRecord.getTrackingId());
        dto.setEnrichmentTrail(rawRecord.getEnrichmentTrail());

        if (rawRecord.getContent().length == 0) {
            dto.setContent(null);
            dto.setContentJSON(null);
            dto.setRecordId(recordIdToDTO(rawRecord.getId()));
        } else {
            if (excludeAttributes != null && !excludeAttributes.contains("content")) {
                dto.setContent(rawRecord.getContent());
            }

            if (excludeAttributes != null && !excludeAttributes.contains("contentJSON")) {
                final MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawRecord.getContent());
                dto.setContentJSON(contentToDTO(marcRecord));
            }

            // TODO Enable once DBCkat can handle it
//            // If 'use parent agency' is enabled the id in the record entity might not be the same as the id of the content
//            final RecordIdDTO marcRecordId = getMarcRecordIdDTO(marcRecord);
//            if (marcRecordId != null) {
//                dto.setRecordId(marcRecordId);
//            } else {
//                dto.setRecordId(recordIdToDTO(rawRecord.getId()));
//            }
            dto.setRecordId(recordIdToDTO(rawRecord.getId()));
        }

        return dto;
    }

    public static RecordCollectionDTO recordCollectionToDTO(Map<String, Record> records, List<String> excludeAttributes) throws MarcReaderException {
        List<RecordDTO> dtoList = new ArrayList<>();

        for (Map.Entry<String, Record> entry : records.entrySet()) {
            final Record rawRecord = entry.getValue();

            dtoList.add(recordToDTO(rawRecord, excludeAttributes));
        }

        RecordCollectionDTO dto = new RecordCollectionDTO();
        dto.setRecords(dtoList);

        return dto;
    }

    public static RecordIdDTO recordIdToDTO(RecordId recordId) {
        RecordIdDTO dto = new RecordIdDTO();
        dto.setBibliographicRecordId(recordId.getBibliographicRecordId());
        dto.setAgencyId(recordId.getAgencyId());

        return dto;
    }

    public static ContentDTO contentToDTO(MarcRecord marcRecord) {
        ContentDTO dto = new ContentDTO();

        dto.setLeader(marcRecord.getLeader().getData());

        List<FieldDTO> fieldDTOList = new ArrayList<>();
        for (Field field : marcRecord.getFields()) {
            if (field instanceof DataField) {
                DataField dataField = (DataField) field;
                FieldDTO fieldDTO = new FieldDTO();
                fieldDTO.setName(dataField.getTag());

                String indicators = "";
                if (dataField.getInd1() != null) {
                    indicators += dataField.getInd1();
                } else {
                    indicators += " ";
                }

                if (dataField.getInd2() != null) {
                    indicators += dataField.getInd2();
                } else {
                    indicators += " ";
                }

                if (dataField.getInd3() != null) {
                    indicators += dataField.getInd3();
                }

                fieldDTO.setIndicators(indicators);

                List<SubfieldDTO> subfieldDTOList = new ArrayList<>();

                for (SubField subField : dataField.getSubfields()) {
                    SubfieldDTO subfieldDTO = new SubfieldDTO();
                    subfieldDTO.setName("" + subField.getCode());
                    subfieldDTO.setValue(subField.getData());

                    subfieldDTOList.add(subfieldDTO);
                }

                fieldDTO.setSubfields(subfieldDTOList);

                fieldDTOList.add(fieldDTO);
            }

            // TODO Implement other Field types
        }

        dto.setFields(fieldDTOList);

        return dto;
    }

    public static RecordIdCollectionDTO recordIdToCollectionDTO(Set<RecordId> set) {
        RecordIdCollectionDTO dto = new RecordIdCollectionDTO();
        dto.setRecordIds(new ArrayList<>());

        for (RecordId recordId : set) {
            dto.getRecordIds().add(recordIdToDTO(recordId));
        }

        return dto;
    }

    public static RecordHistoryDTO recordMetaDataHistoryToDTO(RecordMetaDataHistory recordMetaDataHistory) {
        RecordHistoryDTO dto = new RecordHistoryDTO();

        dto.setId(recordIdToDTO(recordMetaDataHistory.getId()));
        dto.setCreated(recordMetaDataHistory.getCreated().toString());
        dto.setModified(recordMetaDataHistory.getModified().toString());
        dto.setDeleted(recordMetaDataHistory.isDeleted());
        dto.setMimeType(recordMetaDataHistory.getMimeType());
        dto.setTrackingId(recordMetaDataHistory.getTrackingId());

        return dto;
    }

    public static RecordHistoryCollectionDTO recordHistoryCollectionToDTO(List<RecordMetaDataHistory> metaDataHistoryList) {
        List<RecordHistoryDTO> dtoList = new ArrayList<>();

        for (RecordMetaDataHistory recordMetaDataHistory : metaDataHistoryList) {
            dtoList.add(recordMetaDataHistoryToDTO(recordMetaDataHistory));
        }

        RecordHistoryCollectionDTO dto = new RecordHistoryCollectionDTO();
        dto.setRecordHistoryList(dtoList);

        return dto;
    }

    // TODO un-comment again when DBCkat is ready
//    private static RecordIdDTO getMarcRecordIdDTO(MarcRecord marcRecord) {
//        String bibliographicRecordId = null;
//        Integer agencyId = null;
//        for (Field field: marcRecord.getFields()) {
//            final DataField dataField = (DataField) field;
//            if ("001".equals(dataField.getTag())) {
//                for (SubField subField: dataField.getSubfields()) {
//                    if ('a' == subField.getCode()) {
//                        bibliographicRecordId = subField.getData();
//                    } else if ('b' == subField.getCode()) {
//                        agencyId = Integer.parseInt(subField.getData());
//                    }
//                }
//            }
//        }
//
//        if (bibliographicRecordId != null && agencyId != null) {
//            return new RecordIdDTO(bibliographicRecordId, agencyId);
//        } else {
//            return null;
//        }
//    }

    public static RecordRelationChildrenCollectionDTO recordRelationChildrenCollectionToDTO(Map<RecordId, Set<RecordId>> object) {
        final RecordRelationChildrenCollectionDTO dtoCollection = new RecordRelationChildrenCollectionDTO();
        dtoCollection.setRecordRelationChildrenList(new ArrayList<>());

        for (Map.Entry<RecordId, Set<RecordId>> entry : object.entrySet()) {
            final RecordRelationChildrenDTO dto = new RecordRelationChildrenDTO();
            dto.setRecordIdDTO(recordIdToDTO(entry.getKey()));
            dto.setChildren(new ArrayList<>());

            final Set<RecordId> children = entry.getValue();
            for (RecordId recordId : children) {
                dto.getChildren().add(recordIdToDTO(recordId));
            }

            dtoCollection.getRecordRelationChildrenList().add(dto);
        }

        return dtoCollection;
    }

}
