/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.DataField;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.binding.SubField;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.marcxmerge.MarcXMerger;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordRelationsBean;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.pool.DefaultMarcXMergerPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static dk.dbc.marc.binding.MarcRecord.hasTag;

public class MergerThreadFBS implements Callable<Boolean> {

    private static final Logger LOGGER = LoggerFactory.getLogger(MergerThreadFBS.class);
    private static final List<Integer> EXPANDABLE_AGENCIES = Arrays.asList(190002, 190004, 870970, 870971, 870974);

    private final RawRepoBean rawRepoBean;
    private final Map<String, String> recordSet;
    private final RecordByteWriter writer;
    private final int agencyId;
    private final Mode mode;
    private final MarcXMerger merger;
    private final RecordRelationsBean recordBean;

    MergerThreadFBS(RawRepoBean rawRepoBean, RecordRelationsBean recordRelationsBean, Map<String, String> recordSet, RecordByteWriter writer, int agencyId, String modeAsString) {
        this.rawRepoBean = rawRepoBean;
        this.recordBean = recordRelationsBean;
        this.recordSet = recordSet;
        this.writer = writer;
        this.agencyId = agencyId;
        this.mode = Mode.fromString(modeAsString);
        this.merger = new DefaultMarcXMergerPool().checkOut();
    }

    @Override
    public Boolean call() throws RawRepoException, MarcWriterException, JSONBException, IOException, MarcReaderException, MarcXMergerException, RecordNotFoundException, InternalServerException, SAXException {
        final Map<String, byte[]> autRecords = new HashMap<>();

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
            // Only DBC records can have authority link so we don't need to handle that here
            // Local records is equal to "raw" record
            byte[] local;
            byte[] result;
            if (!marcXchangeBibliographicRecordIds.isEmpty()) {
                List<RecordItem> recordItemList = rawRepoBean.getDecodedContent(marcXchangeBibliographicRecordIds, null, agencyId);
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
            // Enrichments can have DBC parents which have authority links so expanded records have to be handled
            // Enrichments can be returned as raw records
            if (!enrichmentBibliographicRecordIds.isEmpty()) {
                if (Mode.RAW == mode) {
                    MergerThreadCommons.getRecordItemsList(enrichmentBibliographicRecordIds, rawRepoBean, agencyId, LOGGER, writer);
                } else {
                    List<RecordItem> recordItemList = rawRepoBean.getDecodedContent(enrichmentBibliographicRecordIds, 870970, agencyId);
                    RecordId expandableRecordId = null;
                    for (RecordItem item : recordItemList) {
                        if (item != null) {
                            final byte[] common = item.getCommon();
                            local = item.getLocal();

                            result = merger.merge(common, local, true);

                            if (Mode.EXPANDED == mode) {
                                if (EXPANDABLE_AGENCIES.contains(agencyId)) {
                                    expandableRecordId = new RecordId(item.getBibliographicRecordId(), agencyId);
                                } else {
                                    Set<RecordId> relationsSiblings = recordBean.getRelationsSiblingsFromMe(item.getBibliographicRecordId(), agencyId);
                                    for (int expandableAgencyId : EXPANDABLE_AGENCIES) {
                                        RecordId potentialExpandableRecordId = new RecordId(item.getBibliographicRecordId(), expandableAgencyId);
                                        if (relationsSiblings.contains(potentialExpandableRecordId)) {
                                            expandableRecordId = potentialExpandableRecordId;
                                            break;
                                        }
                                    }
                                }

                                if (expandableRecordId != null) {
                                    final Set<RecordId> parents = rawRepoBean.getRelationsParents(expandableRecordId);
                                    result = MergerThreadCommons.getBytes(autRecords, result, rawRepoBean, parents);
                                }
                            }

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

            // Handle holdings
            if (!bibliograhicRecordIdsWithHolding.isEmpty()) {
                List<RecordItem> recordItemList = rawRepoBean.getDecodedContent(bibliograhicRecordIdsWithHolding, null, 870970);
                for (RecordItem item : recordItemList) {
                    if (item != null) {
                        local = item.getLocal();

                        local = MergerThreadCommons.getBytes(autRecords, local, item, mode, rawRepoBean, agencyId);

                        MarcXchangeV1Reader reader = new MarcXchangeV1Reader(new ByteArrayInputStream(local), StandardCharsets.UTF_8);
                        MarcRecord record = reader.read();
                        Optional<DataField> field001 = record.getField(DataField.class, hasTag("001"));
                        if (field001.isPresent()) {
                            DataField dataField = field001.get();
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
                            LOGGER.info(msg);
                            throw new MarcReaderException(msg);
                        }
                    }
                }
            }
        }

        return true;
    }
}
