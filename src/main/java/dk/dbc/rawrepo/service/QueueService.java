/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.dao.RawRepoQueueBean;
import dk.dbc.rawrepo.dto.EnqueueAgencyResponseDTO;
import dk.dbc.rawrepo.dto.EnqueueResultCollectionDTO;
import dk.dbc.rawrepo.dto.EnqueueResultDTO;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueStatCollectionDTO;
import dk.dbc.rawrepo.dto.QueueStatDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import dk.dbc.rawrepo.exception.QueueException;
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
import java.util.List;

@Interceptors(StopwatchInterceptor.class)
@Stateless
@Path("api")
public class QueueService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(QueueService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @EJB
    private RawRepoQueueBean rawRepoQueueBean;

    @GET
    @Path("v1/queue/rules")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueRules() {
        String res;

        try {
            final QueueRuleCollectionDTO queueRuleCollectionDTO = new QueueRuleCollectionDTO();
            final List<QueueRuleDTO> queueRuleDTOList = rawRepoQueueBean.getQueueRules();
            queueRuleCollectionDTO.setQueueRules(queueRuleDTOList);

            res = jsonbContext.marshall(queueRuleCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
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
            final List<String> providers = rawRepoQueueBean.getQueueProviders();
            queueProviderCollectionDTO.setProviders(providers);

            res = jsonbContext.marshall(queueProviderCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

    @GET
    @Path("v1/queue/workers")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueWorkers() {
        String res;

        try {
            final QueueWorkerCollectionDTO queueWorkerCollectionDTO = new QueueWorkerCollectionDTO();
            final List<String> workers = rawRepoQueueBean.getQueueWorkers();
            queueWorkerCollectionDTO.setWorkers(workers);

            res = jsonbContext.marshall(queueWorkerCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

    @GET
    @Path("v1/queue/stats/workers")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueWorkerStats() {
        String res;

        try {
            final QueueStatCollectionDTO queueStatCollectionDTO = new QueueStatCollectionDTO();
            final List<QueueStatDTO> queueStats = rawRepoQueueBean.getQueueStatsByWorker();
            queueStatCollectionDTO.setQueueStats(queueStats);

            res = jsonbContext.marshall(queueStatCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

    @GET
    @Path("v1/queue/stats/agency")
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response getQueueAgencyStats() {
        String res;

        try {
            final QueueStatCollectionDTO queueStatCollectionDTO = new QueueStatCollectionDTO();
            final List<QueueStatDTO> queueStats = rawRepoQueueBean.getQueueStatsByAgency();
            queueStatCollectionDTO.setQueueStats(queueStats);

            res = jsonbContext.marshall(queueStatCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

    @POST
    @Path("v1/queue/{agencyid}/{worker}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response enqueueAgency(@PathParam("agencyid") int agencyId,
                                  @PathParam("worker") String worker,
                                  @QueryParam("enqueue-as") Integer queueAsAgencyId,
                                  @DefaultValue("1000") @QueryParam("priority") int priority) {
        String res;

        try {
            // Validate worker
            if (!rawRepoQueueBean.getQueueWorkers().contains(worker)) {
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode(),
                        String.format("The worker '%s' is invalid", worker)).build();
            }

            // Agencies will not be validated as users are allowed to use weird values
            int count;
            if (queueAsAgencyId == null || queueAsAgencyId == agencyId) {
                LOGGER.debug("Selecting and enqueuing same agencyId");
                count = rawRepoQueueBean.enqueueAgency(agencyId, worker, priority);
            } else {
                LOGGER.debug("Selecting and enqueuing with different agencyIds");
                count = rawRepoQueueBean.enqueueAgency(agencyId, queueAsAgencyId, worker, priority);
            }

            final EnqueueAgencyResponseDTO enqueueAgencyResponseDTO = new EnqueueAgencyResponseDTO();
            enqueueAgencyResponseDTO.setCount(count);

            res = jsonbContext.marshall(enqueueAgencyResponseDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during enqueueAgency", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/{}/{}", agencyId, worker);
        }
    }

    @POST
    @Path("v1/queue/{agencyid}/{bibliographicrecordid}/{provider}")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.APPLICATION_JSON})
    @Timed
    public Response enqueueRecord(@PathParam("agencyid") int agencyId,
                                  @PathParam("bibliographicrecordid") String bibliographicRecordId,
                                  @PathParam("provider") String provider,
                                  @DefaultValue("1000") @QueryParam("priority") int priority,
                                  @DefaultValue("true") @QueryParam("changed") boolean changed,
                                  @DefaultValue("true") @QueryParam("leaf") boolean leaf) {
        String res;

        try {
            // Validate provider
            if (!rawRepoQueueBean.getQueueProviders().contains(provider)) {
                return Response.status(Response.Status.BAD_REQUEST.getStatusCode(),
                        String.format("The provider '%s' is invalid", provider)).build();
            }

            final EnqueueResultCollectionDTO enqueueResultCollectionDTO = new EnqueueResultCollectionDTO();
            final List<EnqueueResultDTO> enqueueResults = rawRepoQueueBean.enqueueRecord(bibliographicRecordId,
                    agencyId,
                    provider,
                    changed,
                    leaf,
                    priority);
            enqueueResultCollectionDTO.setEnqueueResults(enqueueResults);

            res = jsonbContext.marshall(enqueueResultCollectionDTO);

            return Response.ok(res, MediaType.APPLICATION_JSON).build();
        } catch (QueueException | JSONBException ex) {
            LOGGER.error("Exception during enqueueRecord", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/{}/{}/{}", agencyId, bibliographicRecordId, provider);
        }
    }

}
