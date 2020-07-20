/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.RawRepoQueueBean;
import dk.dbc.rawrepo.dto.QueueProviderCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleCollectionDTO;
import dk.dbc.rawrepo.dto.QueueRuleDTO;
import dk.dbc.rawrepo.dto.QueueWorkerCollectionDTO;
import dk.dbc.util.StopwatchInterceptor;
import dk.dbc.util.Timed;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.interceptor.Interceptors;
import javax.ws.rs.GET;
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
            final List<String> providers = rawRepoQueueBean.getQueueProviders();
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
        } catch (RawRepoException | JSONBException ex) {
            LOGGER.error("Exception during getQueueProviders", ex);
            return Response.serverError().build();
        } finally {
            LOGGER.info("v1/queue/providers");
        }
    }

}
