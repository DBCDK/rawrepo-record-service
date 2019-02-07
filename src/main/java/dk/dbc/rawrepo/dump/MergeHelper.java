/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;

public class MergeHelper {

    private CustomMarcXMergerPool customMarcXMergerPool;
    private DefaultMarcXMergerPool defaultMarcXMergerPool;

    public MergeHelper() {
        this.customMarcXMergerPool = new CustomMarcXMergerPool();
        this.defaultMarcXMergerPool = new DefaultMarcXMergerPool();
    }

    public byte[] mergeDBC(byte[] common, byte[] local) throws MarcXMergerException {
        // 001 *b will be that of the common record
        MarcXMerger merger = customMarcXMergerPool.checkOut();

        return merger.merge(common, local, true);
    }

    public byte[] mergeFBS(byte[] common, byte[] local) throws MarcXMergerException {
        // 001 *b will be that of the local record
        MarcXMerger merger = defaultMarcXMergerPool.checkOut();

        return merger.merge(common, local, true);
    }

}
