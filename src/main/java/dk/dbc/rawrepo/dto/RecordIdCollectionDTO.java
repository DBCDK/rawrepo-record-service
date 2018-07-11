/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;

public class RecordIdCollectionDTO {

    List<RecordIdDTO> recordIds;

    public List<RecordIdDTO> getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(List<RecordIdDTO> recordIds) {
        this.recordIds = recordIds;
    }

    public RecordIdCollectionDTO() {

    }
}
