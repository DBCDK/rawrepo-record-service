package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.dto.RecordCollectionDTO;
import dk.dbc.rawrepo.dto.RecordDTO;
import dk.dbc.rawrepo.dto.RecordDTOMapper;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
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
import java.util.Arrays;
import java.util.List;

@Stateless
@Path("api")
public class RecordBulkService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordService.class);
    private final JSONBContext jsonbContext = new JSONBContext();
    private final List<Integer> DBC_AGENCIES = Arrays.asList(870970, 870971, 870979);

    @EJB
    private MarcRecordBean marcRecordBean;

    @EJB
    private RawRepoBean rawRepoBean;

    @GET
    @Path("v1/bibliographicrecordids/{agencyid}")
    @Produces({MediaType.APPLICATION_JSON})
    public Response getBibliographicRecordIds(@PathParam("agencyid") int agencyId,
                                              @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted) {
        String res = "";

        try {
            List<String> bibliographicRecordIdList = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, allowDeleted);

            LOGGER.info("Found {} record ids for agency {} ({} deleted records)", bibliographicRecordIdList.size(), agencyId, allowDeleted ? "including" : "not including");

            RecordIdCollectionDTO dto = new RecordIdCollectionDTO();
            dto.setRecordIds(new ArrayList<>());
            //-----------------------------------------

            for (String bibliographicRecordId : bibliographicRecordIdList) {
                dto.getRecordIds().add(new RecordIdDTO(bibliographicRecordId, agencyId));
                if (DBC_AGENCIES.contains(agencyId)) {
                    dto.getRecordIds().add(new RecordIdDTO(bibliographicRecordId, 191919));
                }
            }

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | RawRepoException ex) {
            LOGGER.error("Exception during getBibliographicRecordIds", ex);
            return Response.status(Response.Status.NOT_FOUND).build();
        } finally {
            LOGGER.info("v1/bibliographicrecordids/{agencyid}");
        }
    }

    @POST
    @Path("vi/records/bulk")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    public Response getRecordsBulk(String request,
                                   @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                   @DefaultValue("false") @QueryParam("exclude-dbc-fields") boolean excludeDBCFields,
                                   @DefaultValue("false") @QueryParam("use-parent-agency") boolean useParentAgency,
                                   @DefaultValue("false") @QueryParam("expand") boolean expand,
                                   @DefaultValue("false") @QueryParam("keep-aut-fields") boolean keepAutFields) {
        String res = "";

        try {
            RecordCollectionDTO dto = new RecordCollectionDTO();
            List<RecordDTO> recordDTOs = new ArrayList<>();

            RecordIdCollectionDTO recordIdCollectionDTO = jsonbContext.unmarshall(request, RecordIdCollectionDTO.class);

            Record rawrepoRecord;
            for (RecordIdDTO idDTO : recordIdCollectionDTO.getRecordIds()) {
                if (expand) {
                    rawrepoRecord = marcRecordBean.getRawRepoRecordExpanded(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency, keepAutFields);
                } else {
                    rawrepoRecord = marcRecordBean.getRawRepoRecordMerged(idDTO.getBibliographicRecordId(), idDTO.getAgencyId(), allowDeleted, excludeDBCFields, useParentAgency);
                }

                MarcRecord marcRecord = RecordObjectMapper.contentToMarcRecord(rawrepoRecord.getContent());

                RecordDTO recordDTO = RecordDTOMapper.recordToDTO(rawrepoRecord, marcRecord);
                recordDTOs.add(recordDTO);
            }
            dto.setRecords(recordDTOs);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();

        } catch (Exception ex) {
            LOGGER.error("Exception during getRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("vi/records/bulk");
        }

    }

}
