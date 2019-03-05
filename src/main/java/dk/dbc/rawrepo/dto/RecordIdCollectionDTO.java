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

    public void initialize() {
        this.iterator = recordIds.iterator();
    }

    public RecordIdDTO next() {
        synchronized (this) {
            return iterator.next();
        }
    }

}
