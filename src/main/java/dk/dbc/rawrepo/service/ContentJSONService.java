package dk.dbc.rawrepo.service;

import dk.dbc.marc.binding.MarcRecord;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import java.util.Collection;

@Stateless
@Path("api")
public class ContentJSONService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(ContentJSONService.class);

    @EJB
    private MarcRecordBean marcRecordBean;

    @GET
    @Path("v2/raw/{agencyid}/{bibliographicrecordid}/json")
    @Produces({MediaType.APPLICATION_JSON})
    public MarcRecord GetRaw(@PathParam("agencyid") int agencyId,
                             @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        try {
            final MarcRecord record = marcRecordBean.getRawRecord(bibliographicRecordId, agencyId);

            return record;
        } finally {
            LOGGER.info("v2/raw/{agencyid}/{bibliographicrecordid}/json");
        }
    }

    @GET
    @Path("v2/merged/{agencyid}/{bibliographicrecordid}/json")
    @Produces({MediaType.APPLICATION_JSON})
    public MarcRecord GetMerged(@PathParam("agencyid") int agencyId,
                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getRecordMergedOrExpanded(bibliographicRecordId, agencyId, false, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return record;
        } finally {
            LOGGER.info("v2/merged/{agencyid}/{bibliographicrecordid}/json");
        }
    }

    @GET
    @Path("v2/expanded/{agencyid}/{bibliographicrecordid}/json")
    @Produces({MediaType.APPLICATION_JSON})
    public MarcRecord GetExpanded(@PathParam("agencyid") int agencyId,
                                  @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                  @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                  @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                  @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getRecordMergedOrExpanded(bibliographicRecordId, agencyId, true, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return record;
        } finally {
            LOGGER.info("v2/expanded/{agencyid}/{bibliographicrecordid}/json");
        }
    }

    @GET
    @Path("v2/collection/{agencyid}/{bibliographicrecordid}/json")
    @Produces({MediaType.APPLICATION_JSON})
    public Collection<MarcRecord> GetCollection(@PathParam("agencyid") int agencyId,
                                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                                @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                                @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                                @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final Collection<MarcRecord> collection = marcRecordBean.getRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return collection;
        } finally {
            LOGGER.info("v2/collection/{agencyid}/{bibliographicrecordid}/json");
        }
    }
}
