/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.QueueBean;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.dto.EnqueueAgencyRequestDTO;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueRecordDTO;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.exception.InternalServerException;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.List;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path("api")
public class QueueService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private RawRepoBean rawRepoBean;

    @EJB
    private QueueBean queueBean;

    @GET
    @Path("v1/queue/rules")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueRules() {
        String res;

        try {
            final QueueRuleCollectionDTO queueRuleCollectionDTO = new QueueRuleCollectionDTO();
            final List<QueueRuleDTO> queueRuleDTOList = rawRepoBean.getQueueRules();
            queueRuleCollectionDTO.setQueueProviders(queueRuleDTOList);

            res = jsonbContext.marshall(queueRuleCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (RawRepoException | JSONBException ex) {
            LOGGER.error("Exception during getQueueRules", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/rules");
        }
    }

    @GET
    @Path("v1/queue/providers")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueProviders() {
        String res;

        try {
            final QueueProviderCollectionDTO queueProviderCollectionDTO = new QueueProviderCollectionDTO();
            final List<String> providers = rawRepoBean.getQueueProviders();
            queueProviderCollectionDTO.setProviders(providers);

            res = jsonbContext.marshall(queueProviderCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (RawRepoException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

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

            int count;
            if (enqueueAgencyRequestDTO.getEnqueueAgencyId() == null ||
                    enqueueAgencyRequestDTO.getEnqueueAgencyId().equals(enqueueAgencyRequestDTO.getSelectAgencyId())) {
                LOGGER.info("Selecting and enqueuing same agencyId");
                count = rawRepoBean.enqueueAgency(enqueueAgencyRequestDTO.getSelectAgencyId(),
                        enqueueAgencyRequestDTO.getWorker());
            } else {
                LOGGER.info("Selecting and enqueuing with different agencyIds");
                count = rawRepoBean.enqueueAgency(enqueueAgencyRequestDTO.getSelectAgencyId(),
                        enqueueAgencyRequestDTO.getEnqueueAgencyId(),
                        enqueueAgencyRequestDTO.getWorker());
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
            queueBean.enqueueRecord(enqueueRecordDTO.getBibliographicRecordId(),
                    enqueueRecordDTO.getAgencyId(),
                    enqueueRecordDTO.getProvider(),
                    enqueueRecordDTO.isChanged(),
                    enqueueRecordDTO.isLeaf());

            return Response.ok().build();
        } catch (InternalServerException | RawRepoException ex) {
            LOGGER.error("Exception during enqueueRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/record");
        }
    }
}
