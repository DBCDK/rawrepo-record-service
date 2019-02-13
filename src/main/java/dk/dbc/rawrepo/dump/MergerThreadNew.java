package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.pool.CustomMarcXMergerPool;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.Callable;

public class MergerThreadNew implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadNew.class);

    private RawRepoBean bean;
    private BibliographicIdResultSet recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private AgencyType agencyType;
    private MarcXMerger mergerDefault;
    private MarcXMerger mergerDBC;
    private Params params;

    MergerThreadNew(RawRepoBean bean, BibliographicIdResultSet recordSet, RecordByteWriter writer, int agencyId, AgencyType agencyType, Params params) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.agencyType = agencyType;

        this.mergerDefault = new DefaultMarcXMergerPool().checkOut();
        this.mergerDBC = new CustomMarcXMergerPool().checkOut();
        this.params = params;
    }

    @Override
    public Boolean call() throws Exception {
        try {
            List<String> bibliographicRecordIdList;
            byte[] result, common, local;

            if (agencyType == AgencyType.DBC) {
                LOGGER.info("AgencyType.DBC");
                do {
                    bibliographicRecordIdList = recordSet.next();

                    if (bibliographicRecordIdList != null) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, agencyId, 191919, params);
                        LOGGER.info("Got {} RecordItems", recordItemList.size());
                        for (RecordItem item : recordItemList) {
                            common = item.getCommon();
                            local = item.getLocal();
                            result = mergerDBC.merge(common, local, true);
                            writer.write(result);
                        }
                    }
                } while (bibliographicRecordIdList != null);
            } else if (agencyType == AgencyType.FBS) {
                LOGGER.info("AgencyType.FBS");
                do {
                    bibliographicRecordIdList = recordSet.next();

                    if (bibliographicRecordIdList != null) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, 870970, agencyId, params);
                        LOGGER.info("Got {} RecordItems", recordItemList.size());
                        for (RecordItem item : recordItemList) {
                            if (item != null) {
                                common = item.getCommon();
                                local = item.getLocal();
                                if (common == null || common.length == 0) {
                                    result = local;
                                } else {
                                    result = mergerDefault.merge(common, local, true);
                                }
                                writer.write(result);
                            }
                        }
                    }
                } while (bibliographicRecordIdList != null);
            } else {
                LOGGER.info("AgencyType.LOCAL");
                do {
                    bibliographicRecordIdList = recordSet.next();

                    if (bibliographicRecordIdList != null) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(bibliographicRecordIdList, null, agencyId, params);
                        LOGGER.info("Got {} RecordItems", recordItemList.size());
                        for (RecordItem item : recordItemList) {
                            if (item != null) {
                                result = item.getLocal();
                                writer.write(result);
                            }
                        }
                    }
                } while (bibliographicRecordIdList != null);
            }

            return true;
        } catch (SQLException | MarcXMergerException | IOException | MarcReaderException | MarcWriterException |
                JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}
