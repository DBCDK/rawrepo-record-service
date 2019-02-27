package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class BibliographicIdResultSet {
    private static final Logger LOGGER = LoggerFactory.getLogger(BibliographicIdResultSet.class);

    private RawRepoBean bean;
    private List<String> bibliographicRecordIdList;
    private int sliceSize;
    private int index;
    private boolean initialized = false;
    private Params params;

    public BibliographicIdResultSet(RawRepoBean bean, int sliceSize, Params params) {
        this.bibliographicRecordIdList = new ArrayList<>();
        this.bean = bean;
        this.sliceSize = sliceSize;
        this.params = params;
    }

    public void fetchRecordIds(int agencyId, AgencyType agencyType) throws DumpException, RawRepoException {
        if (initialized) {
            throw new DumpException("Already initialized");
        }

        if (agencyType == AgencyType.DBC) {
            fetchRecordIdsForEnrichments(agencyId, 191919);
        } else if (agencyType == AgencyType.FBS) {
            if (params.getRecordType().contains(RecordType.ENRICHMENT.toString())) {
                fetchRecordIdsForEnrichments(870970, agencyId);
            }

            if (params.getRecordType().contains(RecordType.LOCAL.toString())) {
                fetchRecordIdsForLocal(agencyId);
            }

            // TODO Implement Holdings
        } else { // Local records
            fetchRecordIdsForLocal(agencyId);
        }

        initialized = true;
    }


    private void fetchRecordIdsForEnrichments(int commonAgencyId, int localAgencyId) throws RawRepoException {
        this.bibliographicRecordIdList.addAll(bean.getBibliographicRecordIdsForEnrichmentAgency(commonAgencyId, localAgencyId));
        initialized = true;
    }

    private void fetchRecordIdsForLocal(int localAgencyId) throws RawRepoException {
        this.bibliographicRecordIdList.addAll(bean.getBibliographicRecordIdsForLocalAgency(localAgencyId));
        initialized = true;
    }

    public int size() throws DumpException {
        if (!initialized) {
            throw new DumpException("Not initialized");
        }

        return bibliographicRecordIdList.size();
    }

    public List<String> next() throws DumpException {
        if (!initialized) {
            throw new DumpException("Not initialized");
        }

        synchronized (this) {
            // We are already at the end
            if (this.bibliographicRecordIdList.size() == index) {
                LOGGER.info("Empty");
                return null;
            }

            List<String> slice;

            // Final slice
            if (this.bibliographicRecordIdList.size() < index + sliceSize) {
                LOGGER.info("Last slice");

                slice = bibliographicRecordIdList.subList(index, bibliographicRecordIdList.size());
                index = bibliographicRecordIdList.size();
            } else {
                LOGGER.info("Normal");

                slice = this.bibliographicRecordIdList.subList(index, index + sliceSize);
                index += sliceSize;
            }
            return slice;
        }
    }


}
