/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import dk.dbc.rawrepo.dto.RecordDTO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class RecordDTOCollection {

    RecordDTO[] records;

    public RecordDTO[] getRecords() {
        return records;
    }

    public HashMap<String, RecordDTO> toMap() {
        return Arrays.stream(records)
                .collect(
                        Collectors.toMap(
                                record -> record.getRecordId().getBibliographicRecordId(),
                                record -> record,
                                (first, second) -> second,
                                HashMap::new));
    }
}
