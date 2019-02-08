/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.util.Timed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import javax.ejb.EJB;
import javax.ejb.Stateless;
import javax.enterprise.concurrent.ManagedExecutorService;
import javax.inject.Inject;
import javax.sql.DataSource;
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
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

@Stateless
@Path("api")
public class DumpService {
    private static final Logger LOGGER = LoggerFactory.getLogger(DumpService.class);

    @Inject
    @ConfigProperty(name = "DUMP_THREAD_COUNT", defaultValue = "8")
    private String THREAD_COUNT;
    private int threadCount;

    @Inject
    @ConfigProperty(name = "DUMP_FETCH_SIZE", defaultValue = "50")
    private String FETCH_SIZE;
    private int fetchSize;

    @Resource(lookup = "jdbc/rawrepo")
    private DataSource dataSource;

    @Resource(lookup = "java:comp/DefaultManagedExecutorService")
    private ManagedExecutorService executor;

    @EJB
    private OpenAgencyBean openAgency;

    // Outstanding issues
    // TODO Look at second look class naming
    // TODO Rethink the date from/to logic (time doesn't work?!)
    // TODO Implement Holdings functionality
    // TODO Implement dry-run functionality to get row count
    // TODO Implement readme and create wrapper script

    @PostConstruct
    public void postConstruct() {
        try {
            threadCount = Integer.parseInt(THREAD_COUNT);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Could not parse DUMP_THREAD_COUNT");
        }

        try {
            fetchSize = Integer.parseInt(FETCH_SIZE);
        } catch (NumberFormatException e) {
            throw new RuntimeException("Could not parse DUMP_FETCH_SIZE");
        }
    }

    @POST
    @Path("v1/dump")
    @Consumes({MediaType.APPLICATION_JSON})
    @Produces({MediaType.TEXT_PLAIN})
    public Response dumpLibraryRecords(Params params) {
        // The service is meant to be called from curl, so the error message should be easy to read.
        // Therefor the message is simple text instead of JSON or HTML
        List<String> validateResponse = params.validate(openAgency.getService());
        if (validateResponse.size() > 0) {
            StringBuilder sb = new StringBuilder();
            sb.append("Validation errors: \n");
            for (String msg : validateResponse) {
                sb.append(msg).append("\n");
            }

            return Response.status(400).entity(sb.toString()).build();
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
                            try (Connection connection = dataSource.getConnection();
                                 RecordResultSet resultSet = new RecordResultSet(connection, params, agencyId, agencyType, fetchSize)) {

                                List<Callable<Boolean>> threadList = new ArrayList<>();
                                for (int i = 0; i < threadCount; i++) {
                                    threadList.add(new MergerThread(resultSet, recordByteWriter, agencyType));
                                }

                                executor.invokeAll(threadList);
                            }
                        }
                    } catch (SQLException | OpenAgencyException | InterruptedException e) {
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
