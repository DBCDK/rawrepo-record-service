package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.writer.DanMarc2LineFormatWriter;
import dk.dbc.marc.writer.Iso2709MarcRecordWriter;
import dk.dbc.marc.writer.JsonWriter;
import dk.dbc.marc.writer.MarcWriterException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordBean;
import dk.dbc.rawrepo.RecordHistoryBean;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RecordMetaDataHistory;
import dk.dbc.rawrepo.RecordRelationsBean;
import dk.dbc.rawrepo.RecordSimpleBean;
import dk.dbc.rawrepo.dto.AgencyCollectionDTO;
import dk.dbc.rawrepo.dto.ContentDTO;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordExistsDTO;
import dk.dbc.rawrepo.dump.OutputFormat;
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
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path("api")
public class RecordService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordService.class);
    private final JSONBContext jsonbContext = new JSONBContext();
    private final DanMarc2LineFormatWriter danMarc2LineFormatWriter = new DanMarc2LineFormatWriter();
    private final Iso2709MarcRecordWriter iso2709Writer = new Iso2709MarcRecordWriter();
    private final JsonWriter jsonWriter = new JsonWriter();

    @EJB
    private RecordBean recordBean;

    @EJB
    private RecordSimpleBean recordSimpleBean;

    @EJB
    private RecordRelationsBean recordRelationsBean;

    @EJB
    private RecordHistoryBean historyBean;

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
        String res;

        try {
            final Record record = getRawRepoRecord(agencyId, bibliographicRecordId, mode, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);

            if (record == null) {
                return Response.status(Response.Status.NO_CONTENT).build();
            }

            RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record, excludeAttributes);

            res = jsonbContext.marshall(recordDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | MarcReaderException | InternalServerException ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/fetch")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response fetchRecord(@PathParam("agencyid") int agencyId,
                                @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                @DefaultValue("raw") @QueryParam("mode") Mode mode,
                                @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        String res;
        Record record;
        try {
            if (mode == Mode.EXPANDED) {
                record = recordSimpleBean.fetchRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, useParentAgency);
            } else if (mode == Mode.MERGED) {
                record = recordSimpleBean.fetchRecordMerged(bibliographicRecordId, agencyId, allowDeleted, useParentAgency);
            } else {
                record = recordSimpleBean.fetchRecord(bibliographicRecordId, agencyId);
            }

            RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record, excludeAttributes);

            res = jsonbContext.marshall(recordDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } catch (JSONBException | InternalServerException | MarcReaderException ex) {
            LOGGER.error("Exception during fetchRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/{}/fetch?allow-deleted={}&mode={}&use-parent-agency={}", agencyId, bibliographicRecordId, allowDeleted, mode, useParentAgency);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/content")
    @Timed
    public Response getContent(@PathParam("agencyid") int agencyId,
                               @PathParam("bibliographicrecordid") String bibliographicRecordId,
                               @DefaultValue("merged") @QueryParam("mode") Mode mode,
                               @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                               @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                               @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                               @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields,
                               @DefaultValue("XML") @QueryParam("output-format") OutputFormat format) {
        try {
            final Record record = getRawRepoRecord(agencyId, bibliographicRecordId, mode, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);

            if (record == null) {
                return Response.status(Response.Status.NO_CONTENT).build();
            }

            final MarcRecord marcRecord;
            final String res;
            switch(format) {
                case JSON:
                    // TODO: 10/03/2022 should the JSON format be made to produce the same output as MARC_JSON at some point?
                    marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());
                    final ContentDTO contentDTO = RecordDTOMapper.contentToDTO(marcRecord);
                    res = new String(jsonbContext.marshall(contentDTO).getBytes(StandardCharsets.UTF_8));
                    return Response.ok(res, MediaType.APPLICATION_JSON).build();
                case MARC_JSON:
                    marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());
                    res = new String(jsonWriter.write(marcRecord, StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                    return Response.ok(res, MediaType.APPLICATION_JSON).build();
                case LINE:
                    marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());
                    res = new String(danMarc2LineFormatWriter.write(marcRecord, StandardCharsets.UTF_8));
                    return Response.ok(res, MediaType.TEXT_PLAIN).build();
                case ISO:
                    marcRecord = RecordObjectMapper.contentToMarcRecord(record.getContent());
                    res = new String(iso2709Writer.write(marcRecord, StandardCharsets.UTF_8), StandardCharsets.UTF_8);
                    return Response.ok(res, MediaType.APPLICATION_OCTET_STREAM).build();
                default: // XML and LINE_XML
                    res = new String(record.getContent());
                    return Response.ok(res, MediaType.APPLICATION_XML).build();
            }
        } catch (InternalServerException ex) {
            LOGGER.error("Exception during GetContent", ex);
            return Response.serverError().build();
        } catch (MarcReaderException | JSONBException | MarcWriterException ex) {
            LOGGER.error("Something went wrong while writing output format", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException e) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/content", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/meta")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRecordMetaData(@PathParam("agencyid") int agencyId,
                                      @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                      @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted) {
        String res;

        try {
            final Record record = recordBean.getRawRepoRecordRaw(bibliographicRecordId, agencyId, allowDeleted);

            res = jsonbContext.marshall(RecordDTOMapper.recordMetaDataToDTO(record));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecordMetaData", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException e) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/meta", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/exists")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response recordExists(@PathParam("agencyid") int agencyId,
                                 @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                 @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted) {
        String res;

        try {
            final boolean value = recordSimpleBean.recordExists(bibliographicRecordId, agencyId, allowDeleted);

            RecordExistsDTO dto = new RecordExistsDTO();
            dto.setValue(value);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (JSONBException ex) {
            LOGGER.error("Exception during recordExists", ex);
            return Response.serverError().build();
        } catch (RawRepoException e) {
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/exists", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/parents")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsParents(@PathParam("agencyid") int agencyId,
                                        @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<RecordId> recordIds = recordRelationsBean.getRelationsParents(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRelationsParents", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/parents", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/children")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsChildren(@PathParam("agencyid") int agencyId,
                                         @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<RecordId> recordIds = recordRelationsBean.getRelationsChildren(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRelationsChildren", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/{}/children", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/siblings-from")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsSiblingsFromMe(@PathParam("agencyid") int agencyId,
                                               @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<RecordId> recordIds = recordRelationsBean.getRelationsSiblingsFromMe(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException | RawRepoException ex) {
            LOGGER.error("Exception during getRelationsSiblingsFromMe", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException ex) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/siblings-from", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/siblings-to")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsSiblingsToMe(@PathParam("agencyid") int agencyId,
                                             @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<RecordId> recordIds = recordRelationsBean.getRelationsSiblingsToMe(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException | RawRepoException ex) {
            LOGGER.error("Exception during getRelationsSiblingsToMe", ex);
            return Response.serverError().build();
        } catch (RecordNotFoundException e) {
            return Response.status(Response.Status.NO_CONTENT).build();
        } finally {
            LOGGER.info("v1/record/{}/{}/siblings-to", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/relations-from")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRelationsFrom(@PathParam("agencyid") int agencyId,
                                     @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<RecordId> recordIds = recordRelationsBean.getRelationsFrom(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordIdToCollectionDTO(recordIds));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRelationsFrom", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/{}/relations-from", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{bibliographicrecordid}/all-agencies-for")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getAllAgenciesForBibliographicRecordId(
            @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final Set<Integer> agencySet = recordRelationsBean.getAllAgenciesForBibliographicRecordId(bibliographicRecordId);

            final List<Integer> agencyList = new ArrayList<>(agencySet);
            final AgencyCollectionDTO dto = new AgencyCollectionDTO();
            dto.setAgencies(agencyList);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getAllAgenciesForBibliographicRecordId", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/all-agencies-for", bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/history")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getRecordHistoryList(@PathParam("agencyid") int agencyId,
                                         @PathParam("bibliographicrecordid") String bibliographicRecordId) {
        String res;

        try {
            final List<RecordMetaDataHistory> recordMetaDataHistoryList = historyBean.getRecordHistory(bibliographicRecordId, agencyId);

            res = jsonbContext.marshall(RecordDTOMapper.recordHistoryCollectionToDTO(recordMetaDataHistoryList));

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | InternalServerException ex) {
            LOGGER.error("Exception during getRecordHistoryList", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/{}/history", agencyId, bibliographicRecordId);
        }
    }

    @GET
    @Path("v1/record/{agencyid}/{bibliographicrecordid}/{date}")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getHistoricRecord(@PathParam("agencyid") int agencyId,
                                      @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                      @PathParam("date") String historicDate,
                                      @QueryParam("exclude-attribute") List<String> excludeAttributes) {
        String res;

        try {
            final List<RecordMetaDataHistory> recordMetaDataHistoryList = historyBean.getRecordHistory(bibliographicRecordId, agencyId);
            RecordMetaDataHistory selectedMetaDataHistory = null;

            for (RecordMetaDataHistory recordMetaDataHistory : recordMetaDataHistoryList) {
                // Instead of converting the input date to an Instant or Date it is easier and "safer" to convert the
                // modified date to a String and then compare the strings
                if (historicDate.equals(recordMetaDataHistory.getModified().toString())) {
                    selectedMetaDataHistory = recordMetaDataHistory;
                }
            }

            if (selectedMetaDataHistory != null) {
                Record record = historyBean.getHistoricRecord(selectedMetaDataHistory);

                RecordDTO recordDTO = RecordDTOMapper.recordToDTO(record, excludeAttributes);

                res = jsonbContext.marshall(recordDTO);

                return Response.ok(res, MediaType.APPLICATION_JSON).build();
            } else {
                return Response.status(Response.Status.NO_CONTENT).build();
            }
        } catch (JSONBException | InternalServerException | MarcReaderException ex) {
            LOGGER.error("Exception during getHistoricRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/record/{}/{}/{}", agencyId, bibliographicRecordId, historicDate);
        }
    }

    private Record getRawRepoRecord(int agencyId,
                                    String bibliographicRecordId,
                                    Mode mode,
                                    boolean allowDeleted,
                                    boolean excludeDBCFields,
                                    boolean useParentAgency,
                                    boolean keepAutFields) throws RecordNotFoundException, InternalServerException {
        final Record record;

        if (Mode.RAW.equals(mode)) {
            record = recordBean.getRawRepoRecordRaw(bibliographicRecordId, agencyId, allowDeleted);
        } else if (Mode.MERGED.equals(mode)) {
            record = recordBean.getRawRepoRecordMerged(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency);
        } else {
            record = recordBean.getRawRepoRecordExpanded(bibliographicRecordId, agencyId, allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
        }

        return record;
    }

    public enum Mode {
        RAW("raw"),
        MERGED("merged"),
        EXPANDED("expanded");

        private final String text;

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
