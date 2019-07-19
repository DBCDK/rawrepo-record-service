/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.rawrepo.MarcRecordBean;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.rawrepo.dto.AgencyCollectionDTO;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Interceptors(StopwatchInterceptor.class)
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
    @Timed
    public Response getRecord(@PathParam("agencyid") int agencyId,
                              @PathParam("bibliographicrecordid") String bibliographicRecordId,
                              @DefaultValue("raw") @QueryParam("mode") Mode mode,
                              @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                              @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                              @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                              @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields,
                              @QueryParam("exclude-attribute") List<String> excludeAttributes) {
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

            if (record == null) {
                return Response.status(Response.Status.NOT_FOUND).build();
            }

            RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record);

            for (String excludeAttribute : excludeAttributes) {
                if ("content".equalsIgnoreCase(excludeAttribute)) {
                    recordDTO.setContent(null);
                }

                if ("contentjson".equalsIgnoreCase(excludeAttribute)) {
                    recordDTO.setContentJSON(null);
                }
            }

            res = jsonbContext.marshall(recordDTO);

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
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/fetch")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response fetchRecord(@PathParam("agencyid") int agencyId,
                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        String res = "";

        try {
            Record record = marcRecordBean.fetchRecord(bibliographicRecordId, agencyId);

            RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record);

            res = jsonbContext.marshall(recordDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException | MarcReaderException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/fetch");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content")
    @Produces({MediaType.APPLICATION_XML})
    @Timed
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
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/content");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/meta")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
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
    @Timed
    public Response recordExists(@PathParam("agencyid") int agencyId,
                                 @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                 @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted) {
        String res = "";

        try {
            final boolean value = marcRecordBean.recordExists(bibliographicRecordId, agencyId, allowDeleted);

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

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/parents")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsParents(@PathParam("agencyid") int agencyId,
                                        @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<RecordId> recordIds = marcRecordBean.getRelationsParents(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/parents");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/children")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsChildren(@PathParam("agencyid") int agencyId,
                                         @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<RecordId> recordIds = marcRecordBean.getRelationsChildren(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/children");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/siblings-from")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsSiblingsFromMe(@PathParam("agencyid") int agencyId,
                                               @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<RecordId> recordIds = marcRecordBean.getRelationsSiblingsFromMe(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/siblings-from");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/siblings-to")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsSiblingsToMe(@PathParam("agencyid") int agencyId,
                                             @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<RecordId> recordIds = marcRecordBean.getRelationsSiblingsToMe(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/siblings-to");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/relations-from")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsFrom(@PathParam("agencyid") int agencyId,
                                     @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<RecordId> recordIds = marcRecordBean.getRelationsFrom(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/relations-from");
        }
    }

    @GET
    @Path("v1/record/{bibliographicrecordid}/all-agencies-for")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getAllAgenciesForBibliographicRecordId(
            @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final Set<Integer> agencySet = marcRecordBean.getAllAgenciesForBibliographicRecordId(bibliographicRecordId);

            final List<Integer> agencyList = new ArrayList<>(agencySet);
            final AgencyCollectionDTO dto = new AgencyCollectionDTO();
            dto.setAgencies(agencyList);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{bibliographicrecordid}/all-agencies-for");
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/history")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRecordHistoryList(@PathParam("agencyid") int agencyId,
                                         @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res = "";

        try {
            final List<RecordMetaDataHistory> recordMetaDataHistoryList = marcRecordBean.getRecordHistory(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordHistoryCollectionToDTO(recordMetaDataHistoryList));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}/siblings-to");
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
