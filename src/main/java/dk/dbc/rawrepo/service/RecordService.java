package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import dk.dbc.rawrepo.dto.RecordMapper;
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
import javax.ws.rs.core.Response;

@Stateless
@Path("api")
public class RecordService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(ContentXMLService.class);

    @EJB
    private MarcRecordBean marcRecordBean;

    private final JSONBContext jsonbContext = new JSONBContext();

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getRecord(@PathParam("agencyid") int agencyId,
                              @PathParam("bibliographicrecordid") String bibliographicRecordId,
                              @DefaultValue("raw") @QueryParam("mode") String mode,
                              @DefaultValue("true") @QueryParam("allow-deleted") boolean allowDeleted,
                              @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                              @DefaultValue("false") @QueryParam("overwrite-common-agency") boolean overwriteCommonAgency,
                              @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields) {
        String res = "";

        try {
            Record record;

            if ("raw".equalsIgnoreCase(mode)) {
                record = marcRecordBean.getRawRecord(bibliographicRecordId, agencyId);
            } else if ("merged".equalsIgnoreCase(mode)) {
                record = marcRecordBean.getRawRecordMergedOrExpanded(bibliographicRecordId, agencyId, false, allowDeleted, excludeDBCFields, overwriteCommonAgency, keepAutFields);
            } else if ("expanded".equalsIgnoreCase(mode)) {
                record = marcRecordBean.getRawRecordMergedOrExpanded(bibliographicRecordId, agencyId, true, allowDeleted, excludeDBCFields, overwriteCommonAgency, keepAutFields);
            } else {
                return Response.serverError().build();
            }

            MarcRecord marcRecord = marcRecordBean.rawRecordToMarcRecord(record);

            res = jsonbContext.marshall(RecordMapper.recordToDTO(record, marcRecord));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException | MarcReaderException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/meta")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getRecordMetaData(@PathParam("agencyid") int agencyId,
                                      @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Record record = marcRecordBean.getRawRecord(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordMapper.recordMetaDataToDTO(record));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/meta");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/exists")
    @Produces({MediaType.APPLICATION_JSON})
    public Response recordExists(@PathParam("agencyid") int agencyId,
                                 @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final boolean value = marcRecordBean.recordExists(bibliographicRecordId, agencyId, false);

            RecordExistsDTO dto = new RecordExistsDTO();
            dto.setValue(value);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException ex) {
            LOGGER.error("Exception during recordExists", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/exists");
        }

    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/exists-maybe-deleted")
    @Produces({MediaType.APPLICATION_JSON})
    public Response recordExistsMaybeDeleted(@PathParam("agencyid") int agencyId,
                                             @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final boolean value = marcRecordBean.recordExists(bibliographicRecordId, agencyId, true);

            RecordExistsDTO dto = new RecordExistsDTO();
            dto.setValue(value);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException ex) {
            LOGGER.error("Exception during recordExistsMaybeDeleted", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/exists-maybe-deleted");
        }

    }


}
