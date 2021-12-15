package dk.dbc.rawrepo.output;

import dk.dbc.marc.binding.MarcRecord;

public interface OutputStreamRecordWriter {
    void write(MarcRecord marcRecord) throws Exception;

}
