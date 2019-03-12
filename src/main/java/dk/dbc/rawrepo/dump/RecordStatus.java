/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import java.util.ArrayList;
import java.util.List;

public enum RecordStatus {
    ACTIVE("ACTIVE"), DELETED("DELETED"), ALL("ALL");

    private String value;

    RecordStatus(String value) {
        this.value = value;
    }

    public static RecordStatus fromString(String s) {
        return RecordStatus.valueOf(s.trim().toUpperCase());
    }

    public static String validValues() {
        List<String> values = new ArrayList<>();

        for (RecordStatus s : RecordStatus.values()) {
            values.add(s.value);
        }

        return String.join("|", values);
    }
}
