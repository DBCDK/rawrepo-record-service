/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

import java.util.Objects;

public class EnqueueResultDTO {

    private String bibliographicRecordId;
    private int agencyId;
    private String worker;
    private boolean queued;

    public EnqueueResultDTO() {

    }

    public EnqueueResultDTO(String bibliographicRecordId, int agencyId, String worker, boolean queued) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.agencyId = agencyId;
        this.worker = worker;
        this.queued = queued;
    }

    public String getBibliographicRecordId() {
        return bibliographicRecordId;
    }

    public void setBibliographicRecordId(String bibliographicRecordId) {
        this.bibliographicRecordId = bibliographicRecordId;
    }

    public int getAgencyId() {
        return agencyId;
    }

    public void setAgencyId(int agencyId) {
        this.agencyId = agencyId;
    }

    public String getWorker() {
        return worker;
    }

    public void setWorker(String worker) {
        this.worker = worker;
    }

    public boolean isQueued() {
        return queued;
    }

    public void setQueued(boolean queued) {
        this.queued = queued;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EnqueueResultDTO that = (EnqueueResultDTO) o;
        return agencyId == that.agencyId &&
                queued == that.queued &&
                Objects.equals(bibliographicRecordId, that.bibliographicRecordId) &&
                Objects.equals(worker, that.worker);
    }

    @Override
    public int hashCode() {
        return Objects.hash(bibliographicRecordId, agencyId, worker, queued);
    }
}
