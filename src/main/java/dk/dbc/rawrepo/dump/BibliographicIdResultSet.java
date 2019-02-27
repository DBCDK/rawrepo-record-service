package dk.dbc.rawrepo.dump;

import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoBean;

import java.util.List;
import java.util.stream.Collectors;

public class BibliographicIdResultSet {
    private List<String> bibliographicRecordIdList;
    private int sliceSize;
    private int index;

    public BibliographicIdResultSet(int agencyId, Params params, int sliceSize, RawRepoBean bean) throws RawRepoException {
        this.sliceSize = sliceSize;

        boolean allowDeleted = params.getRecordStatus().equals(RecordStatus.ALL.name()) ||
                params.getRecordStatus().equals(RecordStatus.DELETED.name());

        if (params.getCreatedTo() == null && params.getCreatedFrom() == null && params.getModifiedTo() == null && params.getModifiedFrom() == null) {
            bibliographicRecordIdList = bean.getBibliographicRecordIdForAgency(agencyId, allowDeleted);
        } else {
            bibliographicRecordIdList = bean.getBibliographicRecordIdForAgencyInterval(agencyId, allowDeleted, params.getCreatedTo(), params.getCreatedFrom(), params.getModifiedTo(), params.getModifiedFrom());
        }
    }

    public int size() {
        return bibliographicRecordIdList.size();
    }

    public List<String> next() {
        synchronized (this) {
            List<String> slice = bibliographicRecordIdList.stream()
                    .skip(index)
                    .limit(sliceSize)
                    .collect(Collectors.toList());

            index += sliceSize;

            return slice;
        }
    }



}
