package dk.dbc.rawrepo.dump;

public class RecordItem {

    private final String bibliographicRecordId;
    private final byte[] common;
    private final byte[] local;

    public RecordItem(String bibliographicRecordId, byte[] common, byte[] local) {
        this.bibliographicRecordId = bibliographicRecordId;
        this.common = (common != null) ? common.clone() : null;
        this.local = (local != null) ? local.clone() : null;
    }

    public byte[] getCommon() {
        return (common != null) ? common.clone() : null;
    }

    public byte[] getLocal() {
        return (local != null) ? local.clone() : null;
    }

    public String getBibliographicRecordId() {
        return bibliographicRecordId;
    }

}
