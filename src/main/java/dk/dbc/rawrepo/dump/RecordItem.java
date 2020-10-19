/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

public class RecordItem {

    private final String bibliographicRecordId;
    private final byte[] common;
    private final byte[] local;

    public RecordItem(String bibliographicRecordId, byte[] common, byte[] local) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.common = common;
        this.local = local;
    }

    public byte[] getCommon() {
        return common;
    }

    public byte[] getLocal() {
        return local;
    }

    public String getBibliographicRecordId() {
        return bibliographicRecordId;
    }

}
