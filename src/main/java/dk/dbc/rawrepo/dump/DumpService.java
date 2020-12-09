/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.jsonb.JSONBContext;
import dk.dbc.jsonb.JSONBException;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.RecordRelationsBean;
import dk.dbc.rawrepo.dao.HoldingsItemsBean;
import dk.dbc.rawrepo.dao.RawRepoBean;
import dk.dbc.rawrepo.dto.ParamsValidationDTO;
import dk.dbc.rawrepo.dto.ParamsValidationItemDTO;
import dk.dbc.rawrepo.dto.RecordIdDTO;
import dk.dbc.vipcore.exception.VipCoreException;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.ws.rs.Consumes;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.WebApplicationException;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
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

    @Inject
    private VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;

    @EJB
    private RawRepoBean rawRepoBean;

    @EJB
    private RecordRelationsBean recordRelationsBean;

    @EJB
    private HoldingsItemsBean holdingsItemsBean;

    @POST
    @Path("v1/dump/dryrun")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    public Response dumpLibraryRecordsDryRun(AgencyParams params) {
        try {
            final List<ParamsValidationItemDTO> paramsValidationItemList = params.validate(vipCoreLibraryRulesConnector);
            if (paramsValidationItemList.size() > 0) {
                final ParamsValidationDTO paramsValidation = new ParamsValidationDTO();
                paramsValidation.setErrors(paramsValidationItemList);
                LOGGER.info("Validation errors: {}", paramsValidation);
                return Response.status(400).entity(jsonbContext.marshall(paramsValidation)).build();
            }
        } catch (JSONBException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        }

        try {
            StreamingOutput output = out -> {
                try {
                    for (Integer agencyId : params.getAgencies()) {
                        final AgencyType agencyType = AgencyType.getAgencyType(vipCoreLibraryRulesConnector, agencyId);
                        final Map<String, String> record = getRecords(agencyId, params);
                        final Map<String, String> holdings = getHoldings(agencyId, agencyType, params, true);

                        final BibliographicIdResultSet bibliographicIdResultSet = new
                                BibliographicIdResultSet(params, agencyType, SLICE_SIZE, record, holdings);

                        out.write(String.format("%s: %s%n", agencyId, bibliographicIdResultSet.size()).getBytes());
                    }
                } catch (VipCoreException | RawRepoException | SQLException | IOException e) {
                    LOGGER.error("Caught exception during write", e);
                    throw new WebApplicationException("Caught exception during write", e);
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
    public Response dumpLibraryRecords(AgencyParams params) {
        // The service is meant to be called from curl, so the error message should be easy to read.
        // Therefor the message is simple text instead of JSON or HTML
        try {
            final List<ParamsValidationItemDTO> paramsValidationItemList = params.validate(vipCoreLibraryRulesConnector);
            if (!paramsValidationItemList.isEmpty()) {
                final ParamsValidationDTO paramsValidation = new ParamsValidationDTO();
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
            StreamingOutput output = out -> {
                try {
                    for (Integer agencyId : params.getAgencies()) {
                        final RecordByteWriter recordByteWriter = new RecordByteWriter(out, params);
                        recordByteWriter.writeHeader();
                        final AgencyType agencyType = AgencyType.getAgencyType(vipCoreLibraryRulesConnector, agencyId);
                        final Map<String, String> record = getRecords(agencyId, params);
                        final Map<String, String> holdings = getHoldings(agencyId, agencyType, params, false);

                        LOGGER.info("Opening connection and RecordResultSet...");
                        final BibliographicIdResultSet bibliographicIdResultSet = new
                                BibliographicIdResultSet(params, agencyType, SLICE_SIZE, record, holdings);

                        LOGGER.info("Found {} records", bibliographicIdResultSet.size());
                        List<Callable<Boolean>> threadList = new ArrayList<>();

                        int loopCount = 0;

                        do {
                            loopCount++;

                            if (agencyType == AgencyType.DBC) {
                                threadList.add(new MergerThreadDBC(rawRepoBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId, params.getMode()));
                            } else if (agencyType == AgencyType.FBS) {
                                threadList.add(new MergerThreadFBS(rawRepoBean, recordRelationsBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId, params.getMode()));
                            } else {
                                threadList.add(new MergerThreadLocal(rawRepoBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId));
                            }

                            // Execute the threads when either the outstanding thread count has reached max or it is the last loop
                            if (loopCount % MAX_THREAD_COUNT == 0 || !bibliographicIdResultSet.hasNext()) {
                                final List<Future<Boolean>> futures = executor.invokeAll(threadList);
                                for (Future<Boolean> f : futures) {
                                    try {
                                        f.get(); // We don't care about the result, we just want to see if there was an exception during execution
                                    } catch (ExecutionException e) {
                                        LOGGER.error("Caught exception in a thread", e.getCause());
                                        throw new WebApplicationException(e.getMessage(), e);
                                    }
                                }
                                threadList = new ArrayList<>(); // Reset list to clean up old done threads
                            }
                        } while (bibliographicIdResultSet.hasNext());

                        recordByteWriter.writeFooter();
                    }
                } catch (VipCoreException | InterruptedException | RawRepoException | SQLException | IOException ex) {
                    LOGGER.error("Caught exception during write", ex);
                    throw new WebApplicationException("Caught exception during write", ex);
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

    @POST
    @Path("v1/dump/record")
    @Consumes({MediaType.TEXT_PLAIN})
    @Produces({MediaType.TEXT_PLAIN})
    public Response dumpSingleRecords(String input,
                                      @DefaultValue("UTF-8") @QueryParam("output-encoding") String outputEncoding,
                                      @DefaultValue("LINE") @QueryParam("output-format") String outputFormat,
                                      @DefaultValue("MERGED") @QueryParam("mode") String mode) {
        LOGGER.info(input);
        final RecordParams params = new RecordParams();
        params.setOutputEncoding(outputEncoding);
        params.setOutputFormat(outputFormat);
        params.setMode(mode);
        // The service is meant to be called from curl, so the error message should be easy to read.
        // Therefor the message is simple text instead of JSON or HTML
        try {
            final List<RecordIdDTO> recordIdDTOs = new ArrayList<>();
            final BufferedReader bufReader = new BufferedReader(new StringReader(input));
            String line;
            while ((line = bufReader.readLine()) != null) {
                String[] vars = line.split(":");
                recordIdDTOs.add(new RecordIdDTO(vars[0], Integer.parseInt(vars[1])));
            }

            params.setRecordIds(recordIdDTOs);

            final List<ParamsValidationItemDTO> paramsValidationItemList = params.validate();
            LOGGER.info("Dump single dumping {} records", params.getRecordIds().size());
            LOGGER.info(params.toString());
            if (!paramsValidationItemList.isEmpty()) {
                final ParamsValidationDTO paramsValidation = new ParamsValidationDTO();
                paramsValidation.setErrors(paramsValidationItemList);
                LOGGER.info("Validation errors: {}", paramsValidation);
                return Response.status(400).entity(jsonbContext.marshall(paramsValidation)).build();
            }
        } catch (JSONBException | IOException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        }

        LOGGER.info("Got request: {}", params);

        try {
            StreamingOutput output = out -> {
                try {
                    for (Integer agencyId : params.getAgencies()) {
                        final RecordByteWriter recordByteWriter = new RecordByteWriter(out, params);
                        recordByteWriter.writeHeader();
                        final AgencyType agencyType = AgencyType.getAgencyType(vipCoreLibraryRulesConnector, agencyId);
                        final Map<String, String> record = getRecords(agencyId, params);

                        LOGGER.info("Opening connection and RecordResultSet...");
                        final BibliographicIdResultSet bibliographicIdResultSet = new
                                BibliographicIdResultSet(SLICE_SIZE, record);

                        LOGGER.info("Found {} records", bibliographicIdResultSet.size());
                        List<Callable<Boolean>> threadList = new ArrayList<>();

                        int loopCount = 0;

                        do {
                            loopCount++;

                            if (agencyType == AgencyType.DBC) {
                                threadList.add(new MergerThreadDBC(rawRepoBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId, params.getMode()));
                            } else if (agencyType == AgencyType.FBS) {
                                threadList.add(new MergerThreadFBS(rawRepoBean, recordRelationsBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId, params.getMode()));
                            } else {
                                threadList.add(new MergerThreadLocal(rawRepoBean, bibliographicIdResultSet.next(), recordByteWriter, agencyId));
                            }

                            // Execute the threads when either the outstanding thread count has reached max or it is the last loop
                            if (loopCount % MAX_THREAD_COUNT == 0 || !bibliographicIdResultSet.hasNext()) {
                                final List<Future<Boolean>> futures = executor.invokeAll(threadList);
                                for (Future<Boolean> f : futures) {
                                    try {
                                        f.get(); // We don't care about the result, we just want to see if there was an exception during execution
                                    } catch (ExecutionException e) {
                                        LOGGER.error("Caught exception in thread", e);
                                        throw new WebApplicationException(e.getMessage(), e);
                                    }
                                }
                                threadList = new ArrayList<>(); // Reset list to clean up old done threads
                            }
                        } while (bibliographicIdResultSet.hasNext());

                        recordByteWriter.writeFooter();
                    }
                } catch (VipCoreException | InterruptedException | RawRepoException | IOException ex) {
                    LOGGER.error("Caught exception during write", ex);
                    throw new WebApplicationException("Caught exception during write", ex);
                }
            };

            LOGGER.info("Dump complete");

            return Response.ok(output).build();
        } catch (WebApplicationException ex) {
            LOGGER.error("Caught unexpected exception", ex);
            return Response.status(500).entity("Internal server error. Please see the server log.").build();
        } finally {
            LOGGER.info("v1/dump/record");
        }
    }

    private Map<String, String> getRecords(int agencyId, AgencyParams params) throws RawRepoException {
        Map<String, String> rawrepoRecordMap;

        if (params.getCreatedTo() == null && params.getCreatedFrom() == null && params.getModifiedTo() == null && params.getModifiedFrom() == null) {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgency(agencyId, RecordStatus.fromString(params.getRecordStatus()));
        } else {
            rawrepoRecordMap = rawRepoBean.getBibliographicRecordIdForAgencyInterval(agencyId, RecordStatus.fromString(params.getRecordStatus()), params.getCreatedTo(), params.getCreatedFrom(), params.getModifiedTo(), params.getModifiedFrom());
        }

        return rawrepoRecordMap;
    }

    private Map<String, String> getRecords(int agencyId, RecordParams params) throws RawRepoException {
        Map<String, String> rawrepoRecordMap;

        rawrepoRecordMap = rawRepoBean.getMimeTypeForRecordId(params.getBibliographicRecordIdByAgencyId(agencyId), agencyId);

        return rawrepoRecordMap;
    }

    private Map<String, String> getHoldings(int agencyId, AgencyType agencyType, AgencyParams params, boolean exactMatch) throws SQLException, RawRepoException {
        Map<String, String> holdings = null;

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
