/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoQueueBean;
import dk.dbc.rawrepo.dto.EnqueueAgencyRequestDTO;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueRecordDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path("api")
public class EnqueueService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(EnqueueService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private RawRepoQueueBean rawRepoQueueBean;

    @POST
    @Path("v1/enqueue/agency")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response enqueueAgency(EnqueueAgencyRequestDTO enqueueAgencyRequestDTO) {
        String res;

        try {
            if (enqueueAgencyRequestDTO.getSelectAgencyId() == null) {
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode(), "selectAgencyId must be defined").build();
            }

            if (enqueueAgencyRequestDTO.getWorker() == null) {
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode(), "worker must be defined").build();
            }

            int priority = 1500; // Default priority
            if (enqueueAgencyRequestDTO.getPriority() != null) {
                priority = enqueueAgencyRequestDTO.getPriority();
            }

            int count;
            if (enqueueAgencyRequestDTO.getEnqueueAgencyId() == null ||
                    enqueueAgencyRequestDTO.getEnqueueAgencyId().equals(enqueueAgencyRequestDTO.getSelectAgencyId())) {
                LOGGER.info("Selecting and enqueuing same agencyId");
                count = rawRepoQueueBean.enqueueAgency(enqueueAgencyRequestDTO.getSelectAgencyId(),
                        enqueueAgencyRequestDTO.getWorker(),
                        priority);
            } else {
                LOGGER.info("Selecting and enqueuing with different agencyIds");
                count = rawRepoQueueBean.enqueueAgency(enqueueAgencyRequestDTO.getSelectAgencyId(),
                        enqueueAgencyRequestDTO.getEnqueueAgencyId(),
                        enqueueAgencyRequestDTO.getWorker(),
                        priority);
            }

            final EnqueueAgencyResponseDTO enqueueAgencyResponseDTO = new EnqueueAgencyResponseDTO();
            enqueueAgencyResponseDTO.setCount(count);

            res = jsonbContext.marshall(enqueueAgencyResponseDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (RawRepoException | JSONBException ex) {
            LOGGER.error("Exception during enqueueAgency", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/agency");
        }
    }

    @POST
    @Path("v1/enqueue/record")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response enqueueRecord(EnqueueRecordDTO enqueueRecordDTO) {
        try {

            if (enqueueRecordDTO.getPriority() == null) {
                rawRepoQueueBean.enqueueRecord(enqueueRecordDTO.getBibliographicRecordId(),
                        enqueueRecordDTO.getAgencyId(),
                        enqueueRecordDTO.getProvider(),
                        enqueueRecordDTO.isChanged(),
                        enqueueRecordDTO.isLeaf());
            } else {
                rawRepoQueueBean.enqueueRecord(enqueueRecordDTO.getBibliographicRecordId(),
                        enqueueRecordDTO.getAgencyId(),
                        enqueueRecordDTO.getProvider(),
                        enqueueRecordDTO.isChanged(),
                        enqueueRecordDTO.isLeaf(),
                        enqueueRecordDTO.getPriority());
            }

            return Response.ok().build();
        } catch (InternalServerException | RawRepoException ex) {
            LOGGER.error("Exception during enqueueRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/record");
        }
    }
}
