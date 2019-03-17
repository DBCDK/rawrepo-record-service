/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.HoldingsItemsBean;
import dk.dbc.rawrepo.dao.RawRepoBean;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

public class BibliographicIdResultSet {
    private HashMap<String, String> bibliographicRecordIdList = new HashMap<>();
    private int sliceSize;
    private int index;


    public BibliographicIdResultSet(int agencyId, AgencyType agencyType, Params params, int sliceSize, RawRepoBean rawRepoBean, HoldingsItemsBean holdingsItemsBean) throws RawRepoException, SQLException {
        this.sliceSize = sliceSize;

        HashMap<String, String> rawrepoRecordMap;

        if (params.getCreatedTo() == null && params.getCreatedFrom() == null && params.getModifiedTo() == null && params.getModifiedFrom() == null) {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, RecordStatus.fromString(params.getRecordStatus()));
        } else {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgencyInterval(agencyId, RecordStatus.fromString(params.getRecordStatus()), params.getCreatedTo(), params.getCreatedFrom(), params.getModifiedTo(), params.getModifiedFrom());
        }

        if (AgencyType.FBS == agencyType && params.getRecordType().contains(RecordType.HOLDINGS.toString())) {
            HashMap<String, String> holdingsRecordMap = holdingsItemsBean.getRecordIdsWithHolding(agencyId);

            Set<String> bibliographicRecordIdWithHoldings = holdingsRecordMap.keySet();

            Set<String> localBibliographicRecordIds = rawrepoRecordMap.entrySet().stream()
                    .filter(f -> "text/marcxchange".equals(f.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            Set<String> enrichmentBibliographicRecordIds = rawrepoRecordMap.entrySet().stream()
                    .filter(f -> "text/enrichment+marcxchange".equals(f.getValue()))
                    .map(Map.Entry::getKey)
                    .collect(Collectors.toSet());

            // If there are local records with holdings but local records are not included those local records should be used anyway
            if (!params.getRecordType().contains(RecordType.LOCAL.toString())) {
                // Find set of ids of local records without holdings
                localBibliographicRecordIds.removeAll(bibliographicRecordIdWithHoldings);
                // Remove those records without holdings from the rawrepo record set
                rawrepoRecordMap.keySet().removeAll(localBibliographicRecordIds);
            }

            // If there are enrichments with holdings but enrichments are not included those enrichments should be used anyway
            if (!params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
                // Find set of enrichments without holdings
                enrichmentBibliographicRecordIds.removeAll(bibliographicRecordIdWithHoldings);
                // Remove those enrichments without holdings from the rawrepo record set
                rawrepoRecordMap.keySet().removeAll(enrichmentBibliographicRecordIds);
            }

            // Remove all holdings ids where there is a local or enrichment record
            holdingsRecordMap.keySet().removeAll(rawrepoRecordMap.keySet());

            this.bibliographicRecordIdList.putAll(holdingsRecordMap);
        }

        this.bibliographicRecordIdList.putAll(rawrepoRecordMap);
    }

    public int size() {
        return bibliographicRecordIdList.size();
    }

    public boolean hasNext() {
        return index < bibliographicRecordIdList.size();
    }

    public HashMap<String, String> next() {
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
