package dk.dbc.rawrepo.dump;

import dk.dbc.common.records.MarcRecordExpandException;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.common.records.ExpandCommonMarcRecord;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.dao.RawRepoBean;
import org.slf4j.Logger;
import org.xml.sax.SAXException;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MergerThreadCommons {

    // SonarLint S1118 - Utility classes should not have public constructors
    private MergerThreadCommons() {

    }

    static void getRecordItemsList(List<String> bibliographicRecordIdList, RawRepoBean bean, int agencyId, Logger logger, RecordByteWriter writer) throws RawRepoException, IOException, MarcReaderException, JSONBException, MarcWriterException, SAXException {
        byte[] result;
        final List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, null, agencyId);
        logger.info("Got {} RecordItems", recordItemList.size());
        for (RecordItem item : recordItemList) {
            if (item != null) {
                result = item.getLocal();
                writer.write(result);
            }
        }
    }

    static byte[] getBytes(Map<String, byte[]> autRecords, byte[] result, RecordItem item, Mode mode, RawRepoBean bean, int agencyId) throws RawRepoException, MarcReaderException, MarcRecordExpandException {
        if (Mode.EXPANDED == mode) {
            final Set<RecordId> parents = bean.getRelationsParents(new RecordId(item.getBibliographicRecordId(), agencyId));
            result = getBytes(autRecords, result, bean, parents);
        }
        return result;
    }

    static byte[] getBytes(Map<String, byte[]> autRecords, byte[] result, RawRepoBean bean, Set<RecordId> parents) throws RawRepoException, MarcReaderException, MarcRecordExpandException {
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
        return result;
    }

}
