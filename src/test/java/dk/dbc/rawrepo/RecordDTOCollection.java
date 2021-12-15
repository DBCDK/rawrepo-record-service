package dk.dbc.rawrepo;

import dk.dbc.rawrepo.dto.RecordDTO;

import java.util.Arrays;
import java.util.HashMap;
import java.util.stream.Collectors;

public class RecordDTOCollection {

    RecordDTO[] records;

    // This function is required even if IntelliJ claims it is unused
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
