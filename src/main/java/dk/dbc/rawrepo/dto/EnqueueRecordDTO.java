/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dto;

public class EnqueueRecordDTO {
    String bibliographicRecordId;
    int agencyId;
    String provider;
    boolean changed;
    boolean leaf;

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

    public String getProvider() {
        return provider;
    }

    public void setProvider(String provider) {
        this.provider = provider;
    }

    public boolean isChanged() {
        return changed;
    }

    public void setChanged(boolean changed) {
        this.changed = changed;
    }

    public boolean isLeaf() {
        return leaf;
    }

    public void setLeaf(boolean leaf) {
        this.leaf = leaf;
    }
}
