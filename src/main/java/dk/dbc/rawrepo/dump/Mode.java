package dk.dbc.rawrepo.dump;

import java.util.ArrayList;
import java.util.List;

public enum Mode {
    RAW("RAW"), MERGED("MERGED"), EXPANDED("EXPANDED");

    private String value;

    Mode(String value) {
        this.value = value;
    }

    public static Mode fromString(String s) {
        return Mode.valueOf(s.trim().toUpperCase());
    }

    public static String validValues() {
        List<String> values = new ArrayList<>();

        for (Mode s : Mode.values()) {
            values.add(s.value);
        }

        return String.join("|", values);
    }
}
