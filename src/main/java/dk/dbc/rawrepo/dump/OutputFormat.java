/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import java.util.ArrayList;
import java.util.List;

public enum OutputFormat {
    XML("XML"), LINE("LINE"), JSON("JSON"), ISO("ISO"), LINE_XML("LINE_XML");

    private String value;

    OutputFormat(String value) {
        this.value = value;
    }

    public static OutputFormat fromString(String s) {
        return OutputFormat.valueOf(s.trim().toUpperCase());
    }

    public static String validValues() {
        List<String> values = new ArrayList<>();

        for (OutputFormat s : OutputFormat.values()) {
            values.add(s.value);
        }

        return String.join("|", values);
    }
}
