/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.List;
import java.util.Objects;

public class EnqueueResultCollectionDTO {

    List<EnqueueResultDTO> enqueueResults;

    public List<EnqueueResultDTO> getEnqueueResults() {
        return enqueueResults;
    }

    public void setEnqueueResults(List<EnqueueResultDTO> enqueueResults) {
        this.enqueueResults = enqueueResults;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnqueueResultCollectionDTO that = (EnqueueResultCollectionDTO) o;
        return Objects.equals(enqueueResults, that.enqueueResults);
    }

    @Override
    public int hashCode() {
        return Objects.hash(enqueueResults);
    }
}
