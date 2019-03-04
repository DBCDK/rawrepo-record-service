package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

public class MergerThreadLocal implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadLocal.class);

    private RawRepoBean bean;
    private BibliographicIdResultSet recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private Params params;

    MergerThreadLocal(RawRepoBean bean, BibliographicIdResultSet recordSet, RecordByteWriter writer, int agencyId, Params params) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.params = params;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            List<String> bibliographicRecordIdList;
            byte[] result;

            do {
                bibliographicRecordIdList = recordSet.next().entrySet().stream()
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());

                if (bibliographicRecordIdList.size() > 0) {
                    List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, null, agencyId, params);
                    LOGGER.info("Got {} RecordItems", recordItemList.size());
                    for (RecordItem item : recordItemList) {
                        if (item != null) {
                            result = item.getLocal();
                            writer.write(result);
                        }
                    }
                }
            } while (recordSet.hasNext());

            return true;
        } catch (IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}
