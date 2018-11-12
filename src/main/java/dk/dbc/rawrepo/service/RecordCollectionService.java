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
import dk.dbc.rawrepo.dto.RecordCollectionDTO;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@Interceptors({StopwatchInterceptor.class})
@Stateless
@Path("api")
public class RecordCollectionService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordCollectionService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private MarcRecordBean marcRecordBean;

    @GET
    @Path("v1/records/{agencyid}/{bibliographicrecordid}")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRecordCollection(@PathParam("agencyid") int agencyId,
                                        @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                        @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                        @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                        @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                        @DefaultValue("false") @QueryParam("expand") boolean expand,
                                        @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields,
                                        @DefaultValue("false") @QueryParam("exclude-aut-records") boolean excludeAutRecords,
                                        @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        String res;

        try {

            Map<String, Record> collection = marcRecordBean.getRawRepoRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, expand, keepAutFields, excludeAutRecords);

            RecordCollectionDTO dtoList = RecordDTOMapper.recordCollectionToDTO(collection);

            for (String excludeAttribute : excludeAttributes) {
                if ("content".equalsIgnoreCase(excludeAttribute)) {
                    for (RecordDTO recordDTO : dtoList.getRecords()) {
                        recordDTO.setContent(null);
                    }
                }

                if ("contentjson".equalsIgnoreCase(excludeAttribute)) {
                    for (RecordDTO recordDTO : dtoList.getRecords()) {
                        recordDTO.setContentJSON(null);
                    }
                }
            }

            res = jsonbContext.marshall(dtoList);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}");
        }
    }

    @GET
    @Path("v1/records/{agencyid}/{bibliographicrecordid}/content/")
    @Produces({MediaType.APPLICATION_XML})
    @Timed
    public Response getRecordContentCollection(@PathParam("agencyid") int agencyId,
                                               @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                               @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                               @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                               @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                               @DefaultValue("false") @QueryParam("expand") boolean expand,
                                               @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields) {
        String res;

        try {
            Collection<MarcRecord> marcRecords = marcRecordBean.getMarcRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, expand, keepAutFields);

            res = new String(RecordObjectMapper.marcRecordCollectionToContent(marcRecords));

            return Response.ok(res, MediaType.APPLICATION_XML).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{agencyid}/{bibliographicrecordid}");
        }
    }

    @POST
    @Path("v1/records/bulk")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRecordsBulk(String request,
                                   @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                   @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                   @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                   @DefaultValue("false") @QueryParam("expand") boolean expand,
                                   @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields,
                                   @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        String res;

        try {
            RecordCollectionDTO dto = new RecordCollectionDTO();
            List<RecordDTO> recordDTOs = new ArrayList<>();

            final RecordIdCollectionDTO recordIdCollectionDTO = jsonbContext.unmarshall(request, RecordIdCollectionDTO.class);

            Record rawrepoRecord;
            for (RecordIdDTO idDTO : recordIdCollectionDTO.getRecordIds()) {
                if (expand) {
                    rawrepoRecord = marcRecordBean.getRawRepoRecordExpanded(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
                } else {
                    rawrepoRecord = marcRecordBean.getRawRepoRecordMerged(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency);
                }

                // Ignore records that doesn't exist
                if (rawrepoRecord == null) {
                    continue;
                }
                // TODO Collect all failed or missing records and present those in the returned DTO

                final MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawrepoRecord.getContent());

                final RecordDTO recordDTO = RecordDTOMapper.recordToDTO(rawrepoRecord, marcRecord);

                for (String excludeAttribute : excludeAttributes) {
                    if ("content".equalsIgnoreCase(excludeAttribute)) {
                        recordDTO.setContent(null);
                    }

                    if ("contentjson".equalsIgnoreCase(excludeAttribute)) {
                        recordDTO.setContentJSON(null);
                    }
                }

                recordDTOs.add(recordDTO);
            }
            dto.setRecords(recordDTOs);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException | MarcReaderException ex) {
            LOGGER.error("Exception during getRecordsBulk", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (RecordNotFoundException ex) {
            LOGGER.error("Record not found", ex);
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("vi/records/bulk");
        }
    }

}
