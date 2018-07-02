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
public class ContentXMLService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(ContentXMLService.class);

    @EJB
    private MarcRecordBean marcRecordBean;

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content/raw/xml")
    @Produces({MediaType.APPLICATION_XML})
    public MarcRecord GetRaw(@PathParam("agencyid") int agencyId,
                             @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        try {
            final MarcRecord record = marcRecordBean.getMarcRecord(bibliographicRecordId, agencyId);

            return record;
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content/raw/xml");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content/merged/xml")
    @Produces({MediaType.APPLICATION_XML})
    public MarcRecord GetMerged(@PathParam("agencyid") int agencyId,
                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, false, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return record;
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content/merged/xml");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content/expanded/xml")
    @Produces({MediaType.APPLICATION_XML})
    public MarcRecord GetExpanded(@PathParam("agencyid") int agencyId,
                                  @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                  @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                  @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                  @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final MarcRecord record = marcRecordBean.getMarcRecordMergedOrExpanded(bibliographicRecordId, agencyId, true, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return record;
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content/expanded/xml");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content/collection/xml")
    @Produces({MediaType.APPLICATION_XML})
    public Collection<MarcRecord> GetCollection(@PathParam("agencyid") int agencyId,
                                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                                @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                                                @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                                @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency) {
        try {
            final Collection<MarcRecord> collection = marcRecordBean.getMarcRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, overwriteCommonAgency);

            return collection;
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content/collection/xml");
        }
    }
}
