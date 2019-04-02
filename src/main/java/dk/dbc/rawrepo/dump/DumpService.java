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
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

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
            StreamingOutput output = new StreamingOutput() {
                @Override
                public void write(OutputStream out) throws WebApplicationException {
                    try {
                        for (Integer agencyId : params.getAgencies()) {
                            AgencyType agencyType = AgencyType.getAgencyType(openAgency.getService(), agencyId);
                            HashMap<String, String> record = getRecords(agencyId, params);
                            HashMap<String, String> holdings = getHoldings(agencyId, agencyType, params, true);

                            BibliographicIdResultSet bibliographicIdResultSet = new
                                    BibliographicIdResultSet(params, agencyType, SLICE_SIZE, record, holdings);

                            out.write(String.format("%s: %s%n", agencyId, bibliographicIdResultSet.size()).getBytes());
                        }
                    } catch (OpenAgencyException | RawRepoException | SQLException | IOException e) {
                        LOGGER.error("Caught exception during write", e);
                        throw new WebApplicationException("Caught exception during write", e);
                    }
                }
            };

            LOGGER.info("Dryrun complete");

            return Response.ok(output).build();
        } catch (WebApplicationException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        } finally {
            LOGGER.info("v1/dump/dryrun");
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
                            recordByteWriter.writeHeader();
                            AgencyType agencyType = AgencyType.getAgencyType(openAgency.getService(), agencyId);
                            HashMap<String, String> record = getRecords(agencyId, params);
                            HashMap<String, String> holdings = getHoldings(agencyId, agencyType, params, false);

                            LOGGER.info("Opening connection and RecordResultSet...");
                            BibliographicIdResultSet bibliographicIdResultSet = new
                                    BibliographicIdResultSet(params, agencyType, SLICE_SIZE, record, holdings);

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
                                    List<Future<Boolean>> futures = executor.invokeAll(threadList);
                                    for (Future f : futures) {
                                        try {
                                            f.get(); // We don't care about the result, we just want to see if there was an exception during execution
                                        } catch (ExecutionException e) {
                                            throw new WebApplicationException(e.getMessage(), e);
                                        }
                                    }
                            recordByteWriter.writeFooter();
                        }
                    } catch (OpenAgencyException | InterruptedException | RawRepoException | SQLException | IOException ex) {
                        LOGGER.error("Caught exception during write", ex);
                        throw new WebApplicationException("Caught exception during write", ex);
                    }
                }
            };

            LOGGER.info("Dump complete");

            return Response.ok(output).build();
        } catch (WebApplicationException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        } finally {
            LOGGER.info("v1/dump");
        }
    }

    private HashMap<String, String> getRecords(int agencyId, Params params) throws RawRepoException {
        HashMap<String, String> rawrepoRecordMap;

        if (params.getCreatedTo() == null && params.getCreatedFrom() == null && params.getModifiedTo() == null && params.getModifiedFrom() == null) {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, RecordStatus.fromString(params.getRecordStatus()));
        } else {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgencyInterval(agencyId, RecordStatus.fromString(params.getRecordStatus()), params.getCreatedTo(), params.getCreatedFrom(), params.getModifiedTo(), params.getModifiedFrom());
        }

        return rawrepoRecordMap;
    }


    private HashMap<String, String> getHoldings(int agencyId, AgencyType agencyType, Params params, boolean exactMatch) throws SQLException, RawRepoException {
        HashMap<String, String> holdings = null;

        if (AgencyType.FBS == agencyType && params.getRecordType().contains(RecordType.HOLDINGS.toString())) {
            holdings = holdingsItemsBean.getRecordIdsWithHolding(agencyId);

            if (exactMatch) {
                // There can be holdings on things not present in rawrepo. So to get a more exact list we need to check
                // which ids actually exists
                Set<String> rawrepoRecordsIdsWithHoldings = rawRepoBean.getRawrepoRecordsIdsWithHoldings(holdings.keySet(), agencyId);

                holdings.keySet().retainAll(rawrepoRecordsIdsWithHoldings);
            }
        }

        return holdings;
    }

}
