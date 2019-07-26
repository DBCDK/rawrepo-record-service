/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

public class RecordIdDTO {

    private String bibliographicRecordId;
    private int agencyId;

    // Need default constructor as there is another constructor
    public RecordIdDTO() {
    }

    public RecordIdDTO(String bibliographicRecordId, int agencyId) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.agencyId = agencyId;
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

    @Override
    public String toString() {
        return "RecordIdDTO{" +
                "bibliographicRecordId='" + bibliographicRecordId + '\'' +
                ", agencyId=" + agencyId +
                '}';
    }
}
