package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.dto.RecordIdDTO;

import java.util.ArrayList;
import java.util.List;

public class RecordParams extends Params {

    List<RecordIdDTO> recordIds;

    public List<RecordIdDTO> getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(List<RecordIdDTO> recordIds) {
        this.recordIds = recordIds;
    }

    public List<Integer> getAgencies() {
        List<Integer> result = new ArrayList<>();

        for (RecordIdDTO recordIdDTO : recordIds) {
            if (!result.contains(recordIdDTO.getAgencyId())) {
                result.add(recordIdDTO.getAgencyId());
            }
        }

        return result;
    }

    public List<String> getBibliographicRecordIdByAgencyId(int agencyId) {
        List<String> result = new ArrayList<>();

        for (RecordIdDTO recordIdDTO : recordIds) {
            if (recordIdDTO.getAgencyId() == agencyId) {
                result.add(recordIdDTO.getBibliographicRecordId());
            }
        }

        return result;
    }

    public List<ParamsValidationItem> validate() {
        List<ParamsValidationItem> result = validateParams();

        if (this.recordIds == null || this.recordIds.size() == 0) {
            result.add(new ParamsValidationItem("recordIds", "Field is mandatory and must contain at least one record id"));
        }

        return result;
    }

    @Override
    public String toString() {
        return "RecordParams{" +
                "recordIds=" + recordIds +
                ", outputEncoding='" + outputEncoding + '\'' +
                ", outputFormat='" + outputFormat + '\'' +
                '}';
    }
}
