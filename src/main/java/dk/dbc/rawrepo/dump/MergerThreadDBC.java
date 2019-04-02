/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.Callable;

public class MergerThreadDBC implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadDBC.class);

    private RawRepoBean bean;
    private HashMap<String, String> recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private MarcXMerger merger;
    private Params params;

    MergerThreadDBC(RawRepoBean bean, HashMap<String, String> recordSet, RecordByteWriter writer, int agencyId, Params params) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.merger = new CustomMarcXMergerPool().checkOut();
        this.params = params;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            List<String> bibliographicRecordIdList;
            byte[] result, common, local;

            bibliographicRecordIdList = new ArrayList<>(recordSet.keySet());

            if (bibliographicRecordIdList.size() > 0) {
                List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, agencyId, 191919, params);
                LOGGER.info("Got {} RecordItems", recordItemList.size());
                for (RecordItem item : recordItemList) {
                    common = item.getCommon();
                    local = item.getLocal();
                    result = merger.merge(common, local, true);
                    writer.write(result);
                }
            }

            return true;
        } catch (MarcXMergerException | IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}