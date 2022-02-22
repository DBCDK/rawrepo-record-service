package dk.dbc.rawrepo.dto;

import dk.dbc.rawrepo.RecordId;

import java.util.HashSet;
import java.util.Set;

public class RecordObjectMapper {

    public static RecordId recordIdDTOToObject(RecordIdDTO dto) {
        return new RecordId(dto.getBibliographicRecordId(), dto.getAgencyId());
    }

    public static Set<RecordId> recordIdCollectionDTOToObject(RecordIdCollectionDTO dto) {
        final Set<RecordId> result = new HashSet<>();

        for (RecordIdDTO recordIdDTO: dto.getRecordIds()) {
            result.add(recordIdDTOToObject(recordIdDTO));
        }

        return result;
    }

}
