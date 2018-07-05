package dk.dbc.rawrepo.dto;

import java.util.List;

public class RecordCollectionDTO {

    private List<RecordDTO> records;

    public RecordCollectionDTO() {
    }

    public List<RecordDTO> getRecords() {
        return records;
    }

    public void setRecords(List<RecordDTO> records) {
        this.records = records;
    }
}
