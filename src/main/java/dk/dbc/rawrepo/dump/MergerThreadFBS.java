/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.Field;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static dk.dbc.marc.binding.MarcRecord.hasTag;

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
            HashMap<String, String> bibliographicRecordIdList;
            byte[] result, common, local;

            do {
                bibliographicRecordIdList = recordSet.next();

                if (bibliographicRecordIdList.size() > 0) {
                    List<String> marcXchangeBibliographicRecordIds = bibliographicRecordIdList.entrySet()
                            .stream()
                            .filter(entry -> "text/marcxchange".equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    List<String> enrichmentBibliographicRecordIds = bibliographicRecordIdList.entrySet()
                            .stream()
                            .filter(entry -> "text/enrichment+marcxchange".equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    List<String> bibliograhicRecordIdsWithHolding = bibliographicRecordIdList.entrySet()
                            .stream()
                            .filter(entry -> "holdings".equals(entry.getValue()))
                            .map(Map.Entry::getKey)
                            .collect(Collectors.toList());

                    LOGGER.info("Found the following records for agency {}: {} marcXchange records, {} enrichments and {} holdings", agencyId, marcXchangeBibliographicRecordIds.size(), enrichmentBibliographicRecordIds.size(), bibliograhicRecordIdsWithHolding.size());

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

                    // Handle holdings
                    if (bibliograhicRecordIdsWithHolding.size() > 0) {
                        List<RecordItem> recordItemList = bean.getDecodedContent(bibliograhicRecordIdsWithHolding, null, 870970, params);
                        for (RecordItem item : recordItemList) {
                            if (item != null) {
                                local = item.getLocal();

                                MarcXchangeV1Reader reader = new MarcXchangeV1Reader(new ByteArrayInputStream(local), Charset.forName("UTF-8"));
                                MarcRecord record = reader.read();
                                Optional<Field> field001 = record.getField(hasTag("001"));
                                if (field001.isPresent()) {
                                    DataField dataField = (DataField) field001.get();
                                    for (SubField subField : dataField.getSubfields()) {
                                        if ('b' == subField.getCode()) {
                                            subField.setData(Integer.toString(agencyId));
                                        }
                                    }
                                }

                                MarcXchangeV1Writer marcXchangeV1Writer = new MarcXchangeV1Writer();
                                result = marcXchangeV1Writer.write(record, StandardCharsets.UTF_8);
                                writer.write(result);
                            }
                        }
                    }
                }
            } while (recordSet.hasNext());

            return true;
        } catch (MarcXMergerException | IOException | MarcReaderException | MarcWriterException | JSONBException ex) {
            LOGGER.error("Caught exception while merging record: ", ex);
        }

        return null;
    }
}
