/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import java.util.ArrayList;
import java.util.List;

public enum RecordType {
    LOCAL("LOCAL"), ENRICHMENT("ENRICHMENT"), HOLDINGS("HOLDINGS");

    private final String value;

    RecordType(String value) {
        this.value = value;
    }

    public static RecordType fromString(String s) {
        return RecordType.valueOf(s.trim().toUpperCase());
    }

    public static String validValues() {
        List<String> values = new ArrayList<>();

        for (RecordType s : RecordType.values()) {
            values.add(s.value);
        }

        return String.join("|", values);
    }
}
