/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.Iterator;
import java.util.List;

public class RecordIdCollectionDTO {

    private List<RecordIdDTO> recordIds;
    private Iterator<RecordIdDTO> iterator;


    public List<RecordIdDTO> getRecordIds() {
        return recordIds;
    }

    public void setRecordIds(List<RecordIdDTO> recordIds) {
        this.recordIds = recordIds;
    }

    public void initialize() throws Exception {
        if (this.recordIds == null) {
            throw new Exception("RecordIdDTO list is null");
        }

        this.iterator = recordIds.iterator();
    }

    public RecordIdDTO next() throws Exception {
        if (this.iterator == null) {
            throw new Exception("Not initialized");
        }

        synchronized (this) {
            return iterator.next();
        }
    }


}
