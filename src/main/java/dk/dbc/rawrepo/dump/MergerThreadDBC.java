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
import java.util.Map;
import java.util.concurrent.Callable;

public class MergerThreadDBC implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadDBC.class);

    private final RawRepoBean bean;
    private final Map<String, String> recordSet;
    private final RecordByteWriter writer;
    private final int agencyId;
    private final MarcXMerger merger;
    private final Mode mode;

    MergerThreadDBC(RawRepoBean bean, Map<String, String> recordSet, RecordByteWriter writer, int agencyId, String modeAsString) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.mode = Mode.fromString(modeAsString);
        this.merger = new CustomMarcXMergerPool().checkOut();
    }

    @Override
    public Boolean call() throws Exception {
        try {
            final List<String> bibliographicRecordIdList;
            final Map<String, byte[]> autRecords = new HashMap<>();

            bibliographicRecordIdList = new ArrayList<>(recordSet.keySet());

            if (!bibliographicRecordIdList.isEmpty()) {
                byte[] result;
                if (Mode.RAW == mode) {
                    MergerThreadCommons.getRecordItemsList(bibliographicRecordIdList, bean, agencyId, LOGGER, writer);
                } else {
                    // MERGED and EXPANDED are retrieved the same way. The difference is whether the records should be enriched with aut records or not
                    final List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, agencyId, 191919);
                    LOGGER.info("Got {} RecordItems", recordItemList.size());
                    for (RecordItem item : recordItemList) {
                        final byte[] common = item.getCommon();
                        final byte[] local = item.getLocal();
                        final byte[] merged = merger.merge(common, local, true);

                        result = MergerThreadCommons.getBytes(autRecords, merged, item, mode, bean, agencyId);
                        writer.write(result);
                    }
                }
            }
        } catch (MarcXMergerException | IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.info("Caught exception while merging record: ", ex);
        }

        return true;
    }


}
