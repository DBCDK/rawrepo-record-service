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
import dk.dbc.rawrepo.RawRepoException;
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
    private HashMap<String, String> recordSet;
    private RecordByteWriter writer;
    private int agencyId;
    private MarcXMerger merger;

    MergerThreadFBS(RawRepoBean bean, HashMap<String, String> recordSet, RecordByteWriter writer, int agencyId) {
        this.bean = bean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.merger = new DefaultMarcXMergerPool().checkOut();
    }

    @Override
    public Boolean call() throws RawRepoException, MarcWriterException, JSONBException, IOException, MarcReaderException, MarcXMergerException {
        byte[] result, common, local;

        if (recordSet.size() > 0) {
            List<String> marcXchangeBibliographicRecordIds = recordSet.entrySet()
                    .stream()
                    .filter(entry -> "text/marcxchange".equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            List<String> enrichmentBibliographicRecordIds = recordSet.entrySet()
                    .stream()
                    .filter(entry -> "text/enrichment+marcxchange".equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            List<String> bibliograhicRecordIdsWithHolding = recordSet.entrySet()
                    .stream()
                    .filter(entry -> "holdings".equals(entry.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toList());

            LOGGER.info("Found the following records for agency {}: {} marcXchange records, {} enrichments and {} holdings", agencyId, marcXchangeBibliographicRecordIds.size(), enrichmentBibliographicRecordIds.size(), bibliograhicRecordIdsWithHolding.size());

            // Handle local records
            if (marcXchangeBibliographicRecordIds.size() > 0) {
                List<RecordItem> recordItemList = bean.getDecodedContent(marcXchangeBibliographicRecordIds, null, agencyId);
                for (RecordItem item : recordItemList) {
                    if (item != null) {
                        local = item.getLocal();
                        result = local;
                        try {
                            writer.write(result);
                        } catch (MarcReaderException ex) {
                            final String msg = String.format("Failed to parse '%s:%s' because of %s", item.getBibliographicRecordId(), agencyId, ex.getMessage());
                            LOGGER.error(msg);
                            throw new MarcReaderException(msg);
                        }
                    }
                }
            }

            // Handle enrichments
            if (enrichmentBibliographicRecordIds.size() > 0) {
                List<RecordItem> recordItemList = bean.getDecodedContent(enrichmentBibliographicRecordIds, 870970, agencyId);
                for (RecordItem item : recordItemList) {
                    if (item != null) {
                        common = item.getCommon();
                        local = item.getLocal();

                        result = merger.merge(common, local, true);
                        try {
                            writer.write(result);
                        } catch (MarcReaderException ex) {
                            final String msg = String.format("Failed to parse '%s:%s' because of %s", item.getBibliographicRecordId(), agencyId, ex.getMessage());
                            LOGGER.error(msg);
                            throw new MarcReaderException(msg);
                        }
                    }
                }
            }

            // Handle holdings
            if (bibliograhicRecordIdsWithHolding.size() > 0) {
                List<RecordItem> recordItemList = bean.getDecodedContent(bibliograhicRecordIdsWithHolding, null, 870970);
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
                        try {
                            writer.write(result);
                        } catch (MarcReaderException ex) {
                            final String msg = String.format("Failed to parse '%s:%s' because of %s", item.getBibliographicRecordId(), agencyId, ex.getMessage());
                            LOGGER.error(msg);
                            throw new MarcReaderException(msg);
                        }
                    }
                }
            }
        }

        return true;
    }
}
