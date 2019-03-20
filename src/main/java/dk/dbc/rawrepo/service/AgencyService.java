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
import dk.dbc.rawrepo.dump.RecordStatus;
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
import java.util.HashMap;
import java.util.List;

@Interceptors({StopwatchInterceptor.class})
@Stateless
@Path("api")
public class AgencyService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(AgencyService.class);
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
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getBibliographicRecordIds(@PathParam("agencyid") int agencyId,
                                              @DefaultValue("false") @QueryParam("allow-deleted") boolean allowDeleted,
                                              @DefaultValue("false") @QueryParam("internal-agency-handling") boolean internalAgencyHandling,
                                              @QueryParam("created-before") String createdBefore,
                                              @QueryParam("created-after") String createdAfter,
                                              @QueryParam("modified-before") String modifiedBefore,
                                              @QueryParam("modified-after") String modifiedAfter) {
        String res;

        try {
            HashMap<String,String> bibliographicRecordIdList;

            RecordStatus recordStatus = allowDeleted ? RecordStatus.ALL : RecordStatus.ACTIVE;

            if (createdBefore == null && createdAfter == null &&  modifiedBefore == null && modifiedAfter == null) {
                bibliographicRecordIdList = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, recordStatus);
            } else {
                // The created and modified fields are timestamps. So if only the date is set then add time
                if (createdBefore != null && createdBefore.length() == 10) {
                    createdBefore = createdBefore + " 23:59:59";
                }

                if (createdAfter != null && createdAfter.length() == 10) {
                    createdAfter = createdAfter + " 00:00:00";
                }

                if (modifiedBefore != null && modifiedBefore.length() == 10) {
                    modifiedBefore = modifiedBefore + " 23:59:59";
                }

                if (modifiedAfter != null && modifiedAfter.length() == 10) {
                    modifiedAfter = modifiedAfter + " 00:00:00";
                }

                bibliographicRecordIdList = rawRepoBean.getBibliographicRecordIdForAgencyInterval(agencyId, recordStatus, createdBefore, createdAfter, modifiedBefore, modifiedAfter);
            }

            final RecordIdCollectionDTO dto = new RecordIdCollectionDTO();
            dto.setRecordIds(new ArrayList<>());

            int returnAgencyId = agencyId;

            // If internalAgencyHandling is true a list of bibliographicRecordId:191919 is returned instead of bibliographicRecordId:agencyId
            if (internalAgencyHandling && DBC_AGENCIES.contains(agencyId)) {
                returnAgencyId = 191919;
            }

            for (String bibliographicRecordId : bibliographicRecordIdList.keySet()) {
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
