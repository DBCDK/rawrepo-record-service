package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
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
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private MarcRecordBean marcRecordBean;

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getRecord(@PathParam("agencyid") int agencyId,
                              @PathParam("bibliographicrecordid") String bibliographicRecordId,
                              @DefaultValue("raw") @QueryParam("mode") Mode mode,
                              @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                              @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                              @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                              @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields) {
        String res = "";

        try {
            Record record;

            if (Mode.RAW.equals(mode)) {
                record = marcRecordBean.getRawRepoRecordRaw(bibliographicRecordId, agencyId, allowDeleted);
            } else if (Mode.MERGED.equals(mode)) {
                record = marcRecordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency);
            } else if (Mode.EXPANDED.equals(mode)) {
                record = marcRecordBean.getRawRepoRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
            } else {
                return Response.serverError().build();
            }
            LOGGER.info(new String(record.getContent()));
            MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());

            res = jsonbContext.marshall(RecordDTOMapper.recordToDTO(record, marcRecord));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | MarcReaderException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content")
    @Produces({MediaType.APPLICATION_XML})
    public Response GetContent(@PathParam("agencyid") int agencyId,
                               @PathParam("bibliographicrecordid") String bibliographicRecordId,
                               @DefaultValue("merged") @QueryParam("mode") Mode mode,
                               @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                               @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                               @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                               @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields) {
        String res = "";

        try {
            MarcRecord record;

            if (Mode.RAW.equals(mode) || Mode.MERGED.equals(mode)) {
                record = marcRecordBean.getMarcRecordMerged(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency);
            } else {
                record = marcRecordBean.getMarcRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
            }

            res = new String(RecordObjectMapper.marcToContent(record));

            return Response.ok(res, MediaType.APPLICATION_XML).build();
        } catch (InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content/expanded");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/meta")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getRecordMetaData(@PathParam("agencyid") int agencyId,
                                      @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                      @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted) {
        String res = "";

        try {
            final Record record = marcRecordBean.getRawRepoRecordRaw(bibliographicRecordId, agencyId, allowDeleted);

            res = jsonbContext.marshall(RecordDTOMapper.recordMetaDataToDTO(record));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/meta");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/exists")
    @Produces({MediaType.APPLICATION_JSON})
    public Response recordExists(@PathParam("agencyid") int agencyId,
                                 @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                 @DefaultValue("false") @QueryParam("allow-deleted") boolean maybeDeleted) {
        String res = "";

        try {
            final boolean value = marcRecordBean.recordExists(bibliographicRecordId, agencyId, maybeDeleted);

            RecordExistsDTO dto = new RecordExistsDTO();
            dto.setValue(value);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException ex) {
            LOGGER.error("Exception during recordExists", ex);
            return Response.serverError().build();
        } catch (InternalServerException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/exists");
        }
    }

    public enum Mode {
        RAW("raw"),
        MERGED("merged"),
        EXPANDED("expanded");

        private String text;

        Mode(String text) {
            this.text = text;
        }

        public static Mode fromString(String text) {
            for (Mode b : Mode.values()) {
                if (b.text.equalsIgnoreCase(text)) {
                    return b;
                }
            }
            return null;
        }

        public String getText() {
            return this.text;
        }
    }

}
