/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marcrecord.ExpandCommonMarcRecord;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

public class MergerThreadDBC implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadDBC.class);

    private RawRepoBean bean;
    private HashMap<String, String> recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private MarcXMerger merger;
    private Mode mode;

    MergerThreadDBC(RawRepoBean bean, HashMap<String, String> recordSet, RecordByteWriter writer, int agencyId, String modeAsString) {
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
            byte[] result, common, local;

            bibliographicRecordIdList = new ArrayList<>(recordSet.keySet());

            if (bibliographicRecordIdList.size() > 0) {
                if (Mode.RAW == mode) {
                    final List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, null, agencyId);
                    LOGGER.info("Got {} RecordItems", recordItemList.size());
                    for (RecordItem item : recordItemList) {
                        if (item != null) {
                            result = item.getLocal();
                            writer.write(result);
                        }
                    }
                } else {
                    // MERGED and EXPANDED are retrieved the same way. The difference is whether the records should be enriched with aut records or not
                    final List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, agencyId, 191919);
                    LOGGER.info("Got {} RecordItems", recordItemList.size());
                    for (RecordItem item : recordItemList) {
                        common = item.getCommon();
                        local = item.getLocal();
                        result = merger.merge(common, local, true);

                        if (Mode.EXPANDED == mode) {
                            final Set<RecordId> parents = bean.getRelationsParents(new RecordId(item.getBibliographicRecordId(), agencyId));
                            boolean hasAutParents = false;
                            for (RecordId recordId : parents) {
                                if (870979 == recordId.getAgencyId()) {
                                    hasAutParents = true;
                                    if (!autRecords.containsKey(recordId.getBibliographicRecordId())) {
                                        autRecords.put(recordId.getBibliographicRecordId(), bean.fetchRecordContent(recordId));
                                    }
                                }
                            }

                            if (hasAutParents) {
                                result = ExpandCommonMarcRecord.expandRecord(result, autRecords, false);
                            }
                        }
                        writer.write(result);
                    }
                }
            }

            return true;
        } catch (MarcXMergerException | IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}
