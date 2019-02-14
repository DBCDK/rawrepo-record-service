package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.MarcRecord;

public interface SomethingWriter {

    void write(MarcRecord marcRecord) throws Exception;

}
