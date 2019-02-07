/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marcxmerge.MarcXMergerException;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.concurrent.Callable;

public class MergeRunnable implements Callable<Boolean> {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(MergeRunnable.class);

    private RecordResultSet recordSet;
    private RecordByteWriter writer;
    private MergeHelper mergeHelper;
    private AgencyType agencyType;

    MergeRunnable(RecordResultSet recordSet, RecordByteWriter writer, AgencyType agencyType) {
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyType = agencyType;

        this.mergeHelper = new MergeHelper();
    }

    @Override
    public Boolean call() {
         //This function assumes the params object has already been validate so no further checks will be performed
        RecordItem item = null;
        byte[] result, common, local;
        try {
            if (agencyType == AgencyType.DBC) {
                do {
                    item = recordSet.next();
                    if (item != null) {
                        common = item.getCommon();
                        local = item.getLocal();
                        result = mergeHelper.mergeDBC(common, local);
                        writer.write(result);
                    }
                } while (item != null);
            } else if (agencyType == AgencyType.FBS) {
                do {
                    item = recordSet.next();
                    if (item != null) {
                        common = item.getCommon();
                        local = item.getLocal();
                        if (common == null || common.length == 0) {
                            result = local;
                        } else {
                            result = mergeHelper.mergeFBS(common, local);
                        }
                        writer.write(result);
                    }
                } while (item != null);
            } else {
                do {
                    item = recordSet.next();
                    if (item != null) {
                        result = item.getLocal();
                        writer.write(result);
                    }
                } while (item != null);
            }

            return true;
        } catch (SQLException | MarcXMergerException | IOException | MarcReaderException | MarcWriterException |
                JSONBException ex) {
            if (item != null) {
                LOGGER.error("Caught exception while merging record '" + item.getBibliographicRecordId() + "': ", ex);
            } else {
                LOGGER.error("Caught exception while merging record: ", ex);
            }
        }

        return false;
    }

}
