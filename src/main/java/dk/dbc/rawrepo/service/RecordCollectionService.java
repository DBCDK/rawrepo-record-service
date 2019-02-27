/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.rawrepo.MarcRecordBean;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.dto.RecordCollectionDTO;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.output.OutputStreamJsonRecordWriter;
import dk.dbc.rawrepo.output.OutputStreamLineRecordWriter;
import dk.dbc.rawrepo.output.OutputStreamMarcXchangeRecordWriter;
import dk.dbc.rawrepo.output.OutputStreamRecordWriter;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

@Interceptors({StopwatchInterceptor.class})
@Stateless
@Path("api")
public class RecordCollectionService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordCollectionService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private MarcRecordBean marcRecordBean;

    @Inject
    @ConfigProperty(name = "DUMP_THREAD_COUNT", defaultValue = "8")
    private int THREAD_COUNT;

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;

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
        } catch (RecordNotFoundException ex) {
            LOGGER.error("Record not found", ex);
            return Response.status(Response.Status.NOT_FOUND).build();
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

                final RecordDTO recordDTO = RecordDTOMapper.recordToDTO(rawrepoRecord);

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
            LOGGER.info("v1/records/bulk");
        }
    }

    @POST
    @Path("v2/records/bulk")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    @Timed
    public Response getRecordsBulkv2(String request,
                                     @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                     @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                     @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                     @DefaultValue("LINE") @QueryParam("output-format") String outputFormat,
                                     @DefaultValue("UTF-8") @QueryParam("output-encoding") String outputEncoding) {
        try {
            final RecordIdCollectionDTO recordIdCollectionDTO = jsonbContext.unmarshall(request, RecordIdCollectionDTO.class);
            recordIdCollectionDTO.initialize();
            final StreamingOutput output = new StreamingOutput() {
                @Override
                public void write(OutputStream out) throws WebApplicationException {
                    try {
                        final OutputStreamRecordWriter writer = getWriter(outputFormat, out, outputEncoding);
                        final List<Callable<Boolean>> threadList = new ArrayList<>();
                        for (int i = 0; i < THREAD_COUNT; i++) {
                            threadList.add(new dfg(recordIdCollectionDTO, writer, allowDeleted, excludeDBCFields, useParentAgency));
                        }
                        LOGGER.info("{} MergerThreads has been started", THREAD_COUNT);
                        executor.invokeAll(threadList);

                    } catch (OpenAgencyException e) {
                        LOGGER.error("Caught exception during write", e);
                        throw new WebApplicationException("Caught exception during write", e);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            };

            return Response.ok(output).build();
        } catch (Exception ex) {
            LOGGER.error("Exception during getRecordsBulk", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v2/records/bulk");
        }
    }

    private class dfg implements Callable<Boolean> {
        private RecordIdCollectionDTO recordIdCollectionDTO;
        private OutputStreamRecordWriter writer;
        private boolean allowDeleted;
        private boolean excludeDBCFields;
        private boolean useParentAgency;

        public dfg(RecordIdCollectionDTO recordIdCollectionDTO, OutputStreamRecordWriter writer, boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency) {
            this.recordIdCollectionDTO = recordIdCollectionDTO;
            this.writer = writer;
            this.allowDeleted = allowDeleted;
            this.excludeDBCFields = excludeDBCFields;
            this.useParentAgency = useParentAgency;
        }

        @Override
        public Boolean call() throws Exception {
            RecordIdDTO idDTO;

            do {
                idDTO = recordIdCollectionDTO.next();

                if (idDTO != null) {
                    MarcRecord marcRecord = marcRecordBean.getMarcRecordMerged(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency);

                    writer.write(marcRecord);
                }
            } while (idDTO != null);

            return true;
        }
    }

    private static OutputStreamRecordWriter getWriter(String type, OutputStream stream, String encoding) throws Exception {
        if (type.equalsIgnoreCase("LINE")) {
            return new OutputStreamLineRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("JSON")) {
            return new OutputStreamJsonRecordWriter(stream, encoding);
        }

        if (type.equalsIgnoreCase("MARCXCHANGE")) {
            return new OutputStreamMarcXchangeRecordWriter(stream, encoding);
        }

        throw new Exception("Unknown writer type");
    }

}
