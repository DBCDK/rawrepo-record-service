package dk.dbc.rawrepo.pool;

import dk.dbc.marcxmerge.FieldRules;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;

public class CustomMarcXMergerPool extends ObjectPool<MarcXMerger> {
    private final String immutable = "001;010;020;990;991;996";
    private final String overwrite = "004;005;013;014;017;035;036;240;243;247;300;008 009 038 039 100 110 239 245 652 654";

    @Override
    protected MarcXMerger create() {
        final FieldRules customFieldRules = new FieldRules(immutable, overwrite, FieldRules.INVALID_DEFAULT, FieldRules.VALID_REGEX_DANMARC2);

        try {
            return new MarcXMerger(customFieldRules, "USE_PARENT_AGENCY");
        } catch (MarcXMergerException e) {
            e.printStackTrace();
            return null;
        }

    }

    @Override
    public boolean validate(MarcXMerger o) {
        return true;
    }

    @Override
    public void expire(MarcXMerger o) {

    }
}
