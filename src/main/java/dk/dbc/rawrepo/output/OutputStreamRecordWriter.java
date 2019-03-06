/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.output;

import dk.dbc.marc.binding.MarcRecord;

public interface OutputStreamRecordWriter {
    void write(MarcRecord marcRecord) throws Exception;

}
