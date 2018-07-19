/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.dto.AgencyCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdCollectionDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.rawrepo.interceptor.Compress;
import dk.dbc.rawrepo.interceptor.GZIPWriterInterceptor;
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
import java.util.Arrays;
import java.util.List;

@Interceptors({StopwatchInterceptor.class, GZIPWriterInterceptor.class})
@Stateless
@Path("api")
public class AgencyService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(RecordService.class);
    private final JSONBContext jsonbContext = new JSONBContext();
    private final List<Integer> DBC_AGENCIES = Arrays.asList(870970, 870971, 870979);

    @EJB
    private RawRepoBean rawRepoBean;

    @GET
    @Path("v1/agencies")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getAgencies() {
        String res;

        try {
            List<Integer> agencies = rawRepoBean.getAgencies();

            AgencyCollectionDTO dto = new AgencyCollectionDTO();
            dto.setAgencies(agencies);

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | RawRepoException ex) {
            LOGGER.error("Exception during getBibliographicRecordIds", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v1/agency/{agencyid}/recordids");
        }
    }

    @GET
    @Path("v1/agency/{agencyid}/recordids")
    @Compress
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getBibliographicRecordIds(@PathParam("agencyid") int agencyId,
                                              @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                              @DefaultValue("false") @QueryParam("internal-agency-handling") boolean internalAgencyHandling) {
        String res;

        try {
            final List<String> bibliographicRecordIdList = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, allowDeleted);

            final RecordIdCollectionDTO dto = new RecordIdCollectionDTO();
            dto.setRecordIds(new ArrayList<>());

            int returnAgencyId = agencyId;

            // If internalAgencyHandling is true a list of bibliographicRecordId:191919 is returned instead of bibliographicRecordId:agencyId
            if (internalAgencyHandling && DBC_AGENCIES.contains(agencyId)) {
                returnAgencyId = 191919;
            }

            for (String bibliographicRecordId : bibliographicRecordIdList) {
                dto.getRecordIds().add(new RecordIdDTO(bibliographicRecordId, returnAgencyId));
            }

            LOGGER.info("Found {} record ids for agency {} ({} deleted records)", dto.getRecordIds().size(), agencyId, allowDeleted ? "including" : "not including");

            res = jsonbContext.marshall(dto);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (JSONBException | RawRepoException ex) {
            LOGGER.error("Exception during getBibliographicRecordIds", ex);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).build();
        } finally {
            LOGGER.info("v1/agency/{agencyid}/recordids");
        }
    }

}
