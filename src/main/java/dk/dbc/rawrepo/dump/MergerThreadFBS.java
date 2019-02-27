package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class MergerThreadFBS implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadFBS.class);

    private RawRepoBean bean;
    private BibliographicIdResultSet recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private MarcXMerger merger;
    private Params params;

    MergerThreadFBS(RawRepoBean bean, BibliographicIdResultSet recordSet, RecordByteWriter writer, int agencyId, Params params) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.merger = new DefaultMarcXMergerPool().checkOut();
        this.params = params;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            List<String> bibliographicRecordIdList;
            byte[] result, common, local;

            do {
                bibliographicRecordIdList = recordSet.next();

                if (bibliographicRecordIdList != null && bibliographicRecordIdList.size() > 0) {
                    HashMap<String, String> bibliographicRecordIdToMimetype = bean.getMimeTypeForRecordIds(bibliographicRecordIdList, agencyId);
                    LOGGER.info("Got {} bibliographicRecordIds", bibliographicRecordIdList.size());
                    //for (bibliographicRecordIdToMimetype.)

                    List<String> marcXchangeBibliographicRecordIds = bibliographicRecordIdToMimetype.entrySet()
                            .stream()
                            .filter(entry -> "text/marcxchange".equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    List<String> enrichmentBibliographicRecordIds = bibliographicRecordIdToMimetype.entrySet()
                            .stream()
                            .filter(entry -> "text/enrichment+marcxchange".equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    LOGGER.info("Found {} marcXchange records and {} enrichments", marcXchangeBibliographicRecordIds.size(), enrichmentBibliographicRecordIds.size());

                    // Handle local records
                    if (marcXchangeBibliographicRecordIds.size() > 0) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(marcXchangeBibliographicRecordIds, null, agencyId, params);
                        for (RecordItem item : recordItemList) {
                            if (item != null) {
                                local = item.getLocal();
                                result = local;
                                writer.write(result);
                            }
                        }
                    }

                    // Handle enrichments
                    if (enrichmentBibliographicRecordIds.size() > 0) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(enrichmentBibliographicRecordIds, 870970, agencyId, params);
                        for (RecordItem item : recordItemList) {
                            if (item != null) {
                                common = item.getCommon();
                                local = item.getLocal();

                                result = merger.merge(common, local, true);
                                writer.write(result);
                            }
                        }
                    }

                }
            } while (bibliographicRecordIdList != null && bibliographicRecordIdList.size() > 0);


            return true;
        } catch (MarcXMergerException | IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}
