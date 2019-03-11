/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.dao.HoldingsItemsBean;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.util.Timed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Stateless
@Path("api")
public class DumpService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpService.class);
    private final JSONBContext jsonbContext = new JSONBContext();

    @Inject
    @ConfigProperty(name = "DUMP_THREAD_COUNT", defaultValue = "8")
    private int MAX_THREAD_COUNT;

    @Inject
    @ConfigProperty(name = "DUMP_SLICE_SIZE", defaultValue = "1000")
    private int SLICE_SIZE;

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;

    @EJB
    private OpenAgencyBean openAgency;

    @EJB
    private RawRepoBean rawRepoBean;

    @EJB
    private HoldingsItemsBean holdingsItemsBean;

    // Outstanding issues
    // TODO Implement readme and create wrapper script

    @POST
    @Path("v1/dump/dryrun")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    public Response dumpLibraryRecordsDryRun(Params params) {
        try {
            List<ParamsValidationItem> paramsValidationItemList = params.validate(openAgency.getService());
            if (paramsValidationItemList.size() > 0) {
                ParamsValidation paramsValidation = new ParamsValidation();
                paramsValidation.setErrors(paramsValidationItemList);
                LOGGER.info("Validation errors: {}", paramsValidation);
                return Response.status(400).entity(jsonbContext.marshall(paramsValidation)).build();
            }
        } catch (JSONBException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        }

        try {
            int recordCount = 0;

            for (Integer agencyId : params.getAgencies()) {
                AgencyType agencyType = AgencyType.getAgencyType(openAgency.getService(), agencyId);

                BibliographicIdResultSet bibliographicIdResultSet = new
                        BibliographicIdResultSet(agencyId, agencyType, params, SLICE_SIZE, rawRepoBean, holdingsItemsBean);

                recordCount += bibliographicIdResultSet.size();
            }

            return Response.ok(recordCount).build();
        } catch (RawRepoException | OpenAgencyException | SQLException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        }
    }

    @POST
    @Path("v1/dump")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    public Response dumpLibraryRecords(Params params) {
        // The service is meant to be called from curl, so the error message should be easy to read.
        // Therefor the message is simple text instead of JSON or HTML
        try {
            List<ParamsValidationItem> paramsValidationItemList = params.validate(openAgency.getService());
            if (paramsValidationItemList.size() > 0) {
                ParamsValidation paramsValidation = new ParamsValidation();
                paramsValidation.setErrors(paramsValidationItemList);
                LOGGER.info("Validation errors: {}", paramsValidation);
                return Response.status(400).entity(jsonbContext.marshall(paramsValidation)).build();
            }
        } catch (JSONBException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        }

        LOGGER.info("Got request: {}", params);

        try {
            StreamingOutput output = new StreamingOutput() {
                @Override
                public void write(OutputStream out) throws WebApplicationException {
                    try {
                        for (Integer agencyId : params.getAgencies()) {
                            RecordByteWriter recordByteWriter = new RecordByteWriter(out, params);
                            AgencyType agencyType = AgencyType.getAgencyType(openAgency.getService(), agencyId);
                            LOGGER.info("Opening connection and RecordResultSet...");
                            BibliographicIdResultSet bibliographicIdResultSet = new
                                    BibliographicIdResultSet(agencyId, agencyType, params, SLICE_SIZE, rawRepoBean, holdingsItemsBean);

                            LOGGER.info("Found {} records", bibliographicIdResultSet.size());
                            List<Callable<Boolean>> threadList = new ArrayList<>();
                            int threadCount = Math.min(bibliographicIdResultSet.size() / SLICE_SIZE + 1, MAX_THREAD_COUNT);
                            for (int i = 0; i < threadCount; i++) {
                                if (agencyType == AgencyType.DBC) {
                                    threadList.add(new MergerThreadDBC(rawRepoBean, bibliographicIdResultSet, recordByteWriter, agencyId, params));
                                } else if (agencyType == AgencyType.FBS) {
                                    threadList.add(new MergerThreadFBS(rawRepoBean, bibliographicIdResultSet, recordByteWriter, agencyId, params));
                                } else {
                                    threadList.add(new MergerThreadLocal(rawRepoBean, bibliographicIdResultSet, recordByteWriter, agencyId, params));
                                }
                            }
                            LOGGER.info("{} MergerThreads has been started", threadCount);
                            executor.invokeAll(threadList);
                        }
                    } catch (OpenAgencyException | InterruptedException | RawRepoException | SQLException e) {
                        LOGGER.error("Caught exception during write", e);
                        throw new WebApplicationException("Caught exception during write", e);
                    }
                }
            };

            return Response.ok(output).build();
        } catch (WebApplicationException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        } finally {
            LOGGER.info("v1/dump");
        }
    }

    @GET
    @Path("v1/dump/readme")
    @Produces({MediaType.TEXT_PLAIN})
    @Timed
    public Response readMe() {
        String res = "Not yet implemented";

        return Response.ok(res).build();
    }

}
