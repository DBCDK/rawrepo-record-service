package dk.dbc.rawrepo.pool;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;

public class DefaultMarcXMergerPool extends ObjectPool<MarcXMerger> {

    @Override
    protected MarcXMerger create() {
        try {
            return new MarcXMerger();
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
