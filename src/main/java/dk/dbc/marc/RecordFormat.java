package dk.dbc.marc;

/**
 *
 * <pre>
 * This class is copied from the mconv project because mconv can't set as a maven dependency.
 * That problem should be fixed and this class removed.
 * The namespace is the same here as in mconv, so no other code changes should be necessary.
 * See issue <a href="https://dbcjira.atlassian.net/browse/MS-4134">MS-4134</a>
 * </pre>
 */
// TODO Remove class and instead import from dk.dbc.mconv jar
public enum RecordFormat {
    LINE,
    LINE_CONCAT,
    MARCXCHANGE,
    ISO,
    JSONL,
}
