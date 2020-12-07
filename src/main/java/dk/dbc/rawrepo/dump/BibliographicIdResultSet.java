/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class BibliographicIdResultSet {
    private final Map<String, String> bibliographicRecordIdList = new HashMap<>();
    private final int sliceSize;
    private int index;

    public BibliographicIdResultSet(AgencyParams params, AgencyType agencyType, int sliceSize, Map<String, String> records, Map<String, String> holdings) {
        this.sliceSize = sliceSize;

        Set<String> localBibliographicRecordIds = records.entrySet().stream()
                .filter(f -> "text/marcxchange".equals(f.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        Set<String> enrichmentBibliographicRecordIds = records.entrySet().stream()
                .filter(f -> "text/enrichment+marcxchange".equals(f.getValue()))
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());

        if (holdings != null) {
            Set<String> bibliographicRecordIdWithHoldings = holdings.keySet();

            // If there are local records with holdings but local records are not included those local records should be used anyway
            if (!params.getRecordType().contains(RecordType.LOCAL.toString())) {
                // Find set of ids of local records without holdings
                localBibliographicRecordIds.removeAll(bibliographicRecordIdWithHoldings);
                // Remove those records without holdings from the rawrepo record set
                records.keySet().removeAll(localBibliographicRecordIds);
            }

            // If there are enrichments with holdings but enrichments are not included those enrichments should be used anyway
            if (!params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
                // Find set of enrichments without holdings
                enrichmentBibliographicRecordIds.removeAll(bibliographicRecordIdWithHoldings);
                // Remove those enrichments without holdings from the rawrepo record set
                records.keySet().removeAll(enrichmentBibliographicRecordIds);
            }

            // Remove all holdings ids where there is a local or enrichment record
            holdings.keySet().removeAll(records.keySet());

            this.bibliographicRecordIdList.putAll(holdings);
        } else if (agencyType == AgencyType.FBS){
            if (!params.getRecordType().contains(RecordType.LOCAL.toString())) {
                records.keySet().removeAll(localBibliographicRecordIds);
            }

            if (!params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
                records.keySet().removeAll(enrichmentBibliographicRecordIds);
            }
        }

        this.bibliographicRecordIdList.putAll(records);
    }

    public BibliographicIdResultSet(int sliceSize,  Map<String, String> records) {
        this.sliceSize = sliceSize;
        this.bibliographicRecordIdList.putAll(records);
    }

    public int size() {
        return bibliographicRecordIdList.size();
    }

    public boolean hasNext() {
        synchronized (this) {
            return index < bibliographicRecordIdList.size();
        }
    }

    public Map<String, String> next() {
        synchronized (this) {
            HashMap<String, String> slice = bibliographicRecordIdList.entrySet().stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(toMap(
                            Entry::getKey,
                            Entry::getValue,
                            (x, y) -> {
                                throw new AssertionError();
                            },
                            HashMap::new));

            index += sliceSize;

            return slice;
        }
    }


}
