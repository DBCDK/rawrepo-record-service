package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marcxmerge.MarcXMergerException;
import dk.dbc.rawrepo.MarcRecordBean;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordBean;
import dk.dbc.rawrepo.RecordCollectionBean;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordRelationsBean;
import dk.dbc.rawrepo.RecordSimpleBean;
import dk.dbc.rawrepo.dto.RecordCollectionDTO;
import dk.dbc.rawrepo.dto.RecordCollectionDTOv2;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.dto.RecordRelationChildrenCollectionDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.rawrepo.exception.RecordNotFoundException;
import dk.dbc.rawrepo.exception.WebApplicationInvalidInputException;
import dk.dbc.rawrepo.output.OutputStreamRecordWriter;
import dk.dbc.rawrepo.output.OutputStreamWriterUtil;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.annotation.Resource;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

import static dk.dbc.rawrepo.dto.RecordObjectMapper.recordIdCollectionDTOToObject;
import static java.nio.charset.StandardCharsets.UTF_8;

@Interceptors({StopwatchInterceptor.class})
@Stateless
@Path("api")
public class RecordCollectionService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordCollectionService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @Inject
    private MarcRecordBean marcRecordBean;

    @Inject
    private RecordCollectionBean recordCollectionBean;

    @Inject
    private RecordBean recordBean;

    @Inject
    private RecordSimpleBean recordSimpleBean;

    @Inject
    private RecordRelationsBean recordRelationsBean;

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
        try {

            Map<String, Record> collection = recordCollectionBean.getRawRepoRecordCollection(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, expand, keepAutFields, excludeAutRecords);

            return recordCollectionToResponse(excludeAttributes, collection);
        } catch (JSONBException | MarcReaderException | InternalServerException ex) {
            LOGGER.error("Exception during getRecordCollection", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
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
                                               @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields,
                                               @DefaultValue("false") @QueryParam("exclude-aut-records") boolean excludeAutRecords) {
        String res;

        try {
            Collection<MarcRecord> marcRecords = marcRecordBean.getMarcRecordCollection(bibliographicRecordId,
                    agencyId,
                    allowDeleted,
                    excludeDBCFields,
                    useParentAgency,
                    expand,
                    keepAutFields,
                    excludeAutRecords);

            res = new String(RecordObjectMapper.marcRecordCollectionToContent(marcRecords), UTF_8);

            return Response.ok(res, MediaType.APPLICATION_XML).build();
        } catch (MarcReaderException | InternalServerException | MarcXMergerException ex) {
            LOGGER.error("Exception during getRecordContentCollection", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/records/{}/{}/content?allow-deleted={}&exclude-dbc-fields={}&use-parent-agency={}&expand={}&keep-aut-fields={}",
                    agencyId, bibliographicRecordId, allowDeleted, excludeDBCFields, useParentAgency, expand, keepAutFields);
        }
    }

    @GET
    @Path("v1/records/{agencyid}/{bibliographicrecordid}/dataio/")
    @Produces({MediaType.APPLICATION_XML})
    @Timed
    public Response getRecordContentCollectionDataIO(@PathParam("agencyid") int agencyId,
                                                     @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                                     @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                                     @DefaultValue("false") @QueryParam("expand") boolean expand,
                                                     @QueryParam("exclude-attribute") List<String> excludeAttributes,
                                                     @DefaultValue("false") @QueryParam("handle-control-records") boolean handleControlRecords) {
        try {
            final Map<String, Record> collection = marcRecordBean.getDataIOMarcRecordCollection(bibliographicRecordId, agencyId, useParentAgency, expand, handleControlRecords);

            return recordCollectionToResponse(excludeAttributes, collection);
        } catch (MarcReaderException | InternalServerException | JSONBException ex) {
            LOGGER.error("Exception during getRecordContentCollectionDataIO: {}", ex.getMessage(), ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            LOGGER.error("Caught RecordNotFoundException: {}", ex.getMessage(), ex);
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/records/{}/{}/dataio?expand={}&handle-control-records={}", agencyId, bibliographicRecordId, expand, handleControlRecords);
        }
    }

    private Response recordCollectionToResponse(List<String> excludeAttributes, Map<String, Record> collection) throws MarcReaderException, JSONBException {
        String res;
        RecordCollectionDTO dtoList = RecordDTOMapper.recordCollectionToDTO(collection, excludeAttributes);

        res = jsonbContext.marshall(dtoList);

        return Response.ok(res, MediaType.APPLICATION_JSON).build();
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
                    rawrepoRecord = recordBean.getRawRepoRecordExpanded(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
                } else {
                    rawrepoRecord = recordBean.getRawRepoRecordMerged(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency);
                }

                // Ignore records that doesn't exist
                if (rawrepoRecord == null) {
                    continue;
                }
                // TODO Collect all failed or missing records and present those in the returned DTO

                final RecordDTO recordDTO = RecordDTOMapper.recordToDTO(rawrepoRecord, excludeAttributes);

                recordDTOs.add(recordDTO);
            }
            dto.setRecords(recordDTOs);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException | MarcReaderException ex) {
            LOGGER.error("Exception during getRecordsBulk", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
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

            final StreamingOutput output = out -> {
                try {
                    final OutputStreamRecordWriter writer = OutputStreamWriterUtil.getWriter(outputFormat, out, outputEncoding);
                    final List<Callable<Boolean>> threadList = new ArrayList<>();
                    final Iterator<RecordIdDTO> iterator = recordIdCollectionDTO.getRecordIds().iterator();

                    for (int i = 0; i < THREAD_COUNT; i++) {
                        threadList.add(new BulkMergeThread(iterator, writer, allowDeleted, excludeDBCFields, useParentAgency));
                    }

                    LOGGER.info("{} MergerThreads has been started", THREAD_COUNT);
                    executor.invokeAll(threadList);
                } catch (InterruptedException e) {
                    LOGGER.error("Caught exception during write", e);
                    throw new WebApplicationException("Caught exception during write", e);
                }
            };

            return Response.ok(output).build();
        } catch (WebApplicationInvalidInputException ex) {
            LOGGER.error("Invalid input", ex);
            return Response.status(400).entity("Invalid input").build();
        } catch (WebApplicationException | JSONBException ex) {
            LOGGER.error("Exception during getRecordsBulkv2", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v2/records/bulk");
        }
    }

    @POST
    @Path("v1/records/fetch")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response fetchRecordList(String request,
                                    @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                    @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                    @DefaultValue("raw") @QueryParam("mode") RecordService.Mode mode,
                                    @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        final RecordCollectionDTOv2 dto = new RecordCollectionDTOv2();
        final List<RecordDTO> found = new ArrayList<>();
        final List<RecordIdDTO> missing = new ArrayList<>();
        final String res;
        try {
            final RecordIdCollectionDTO recordIdCollectionDTO = jsonbContext.unmarshall(request, RecordIdCollectionDTO.class);

            for (RecordIdDTO recordId : recordIdCollectionDTO.getRecordIds()) {
                fetchRecords(allowDeleted, useParentAgency, mode, excludeAttributes, found, missing, recordId);
            }

            dto.setFound(found);
            dto.setMissing(missing);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | MarcReaderException | InternalServerException ex) {
            LOGGER.error("Exception during fetchRecordList", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v1/records/fetch");
        }
    }

    @POST
    @Path("v1/records/children")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsChildren(RecordIdCollectionDTO recordIdCollectionDTO) {
        final String res;
        try {
            final Set<RecordId> recordIds = recordIdCollectionDTOToObject(recordIdCollectionDTO);
            final Map<RecordId, Set<RecordId>> relationsChildren = recordRelationsBean.getRelationsChildren(recordIds);

            final RecordRelationChildrenCollectionDTO dto = RecordDTOMapper.recordRelationChildrenCollectionToDTO(relationsChildren);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException e) {
            LOGGER.error("Exception during getRelationsChildren", e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v1/records/children");
        }
    }

    private void fetchRecords(boolean allowDeleted, boolean useParentAgency, RecordService.Mode mode, List<String> excludeAttributes, List<RecordDTO> found, List<RecordIdDTO> missing, RecordIdDTO recordId) throws InternalServerException, MarcReaderException {
        Record record;
        try {
            if (mode == RecordService.Mode.EXPANDED) {
                record = recordSimpleBean.fetchRecordExpanded(recordId.getBibliographicRecordId(), recordId.getAgencyId(), allowDeleted, useParentAgency);
            } else if (mode == RecordService.Mode.MERGED) {
                record = recordSimpleBean.fetchRecordMerged(recordId.getBibliographicRecordId(), recordId.getAgencyId(), allowDeleted, useParentAgency);
            } else {
                record = recordSimpleBean.fetchRecord(recordId.getBibliographicRecordId(), recordId.getAgencyId());
            }

            // fetchRecord will return a new empty record if the requested record is not found in the database.
            // One solution would be to check if the record exists but that requires an extra request to the database
            // In order to save a round trip we just check if the record has any content.
            if (record.getContent() != null && record.getContent().length > 0) {
                final RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record, excludeAttributes);
                found.add(recordDTO);
            } else {
                missing.add(recordId);
            }
        } catch (RecordNotFoundException e) {
            missing.add(recordId);
        }
    }

    private class BulkMergeThread implements Callable<Boolean> {
        private final Iterator<RecordIdDTO> iterator;
        private final OutputStreamRecordWriter writer;
        private final boolean allowDeleted;
        private final boolean excludeDBCFields;
        private final boolean useParentAgency;

        public BulkMergeThread(Iterator<RecordIdDTO> iterator, OutputStreamRecordWriter writer, boolean allowDeleted, boolean excludeDBCFields, boolean useParentAgency) {
            this.iterator = iterator;
            this.writer = writer;
            this.allowDeleted = allowDeleted;
            this.excludeDBCFields = excludeDBCFields;
            this.useParentAgency = useParentAgency;
        }

        @Override
        public Boolean call() throws Exception {
            RecordIdDTO idDTO;

            do {
                synchronized (iterator) {
                    if (iterator.hasNext()) {
                        idDTO = iterator.next();
                    } else {
                        idDTO = null;
                    }
                }

                if (idDTO != null) {
                    MarcRecord marcRecord = marcRecordBean.getMarcRecordMerged(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency);

                    writer.write(marcRecord);
                }
            } while (idDTO != null);

            return true;
        }
    }

}
