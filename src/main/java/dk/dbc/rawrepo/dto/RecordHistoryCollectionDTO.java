package dk.dbc.rawrepo.dto;

import java.util.List;

public class RecordHistoryCollectionDTO {

    private List<RecordHistoryDTO> recordHistoryList;

    public List<RecordHistoryDTO> getRecordHistoryList() {
        return recordHistoryList;
    }

    public void setRecordHistoryList(List<RecordHistoryDTO> recordHistoryList) {
        this.recordHistoryList = recordHistoryList;
    }
}
