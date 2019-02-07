/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.dump;

import dk.dbc.openagency.client.LibraryRuleHandler;
import dk.dbc.openagency.client.OpenAgencyException;
import dk.dbc.rawrepo.dao.OpenAgencyBean;
import dk.dbc.util.Timed;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.slf4j.ext.XLogger;
import org.slf4j.ext.XLoggerFactory;

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
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.StreamingOutput;
import java.io.OutputStream;
import java.nio.charset.Charset;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;

@Stateless
@Path("api")
public class DumpService {
    private static final XLogger LOGGER = XLoggerFactory.getXLogger(DumpService.class);

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
    // TODO Optimize merge pool logic (create one pool per thread and use that)
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
        List<String> validateResponse = validateParams(params);
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
                public void write(OutputStream out) {
                    try {
                        for (Integer agencyId : params.getAgencies()) {
                            RecordByteWriter recordByteWriter = new RecordByteWriter(out, params);
                            AgencyType agencyType = getAgencyType(agencyId);
                            try (Connection connection = dataSource.getConnection();
                                 RecordResultSet resultSet = new RecordResultSet(connection, params, agencyId, agencyType, fetchSize)) {

                                List<Callable<Boolean>> threadList = new ArrayList<>();
                                for (int i = 0; i < threadCount; i++) {
                                    threadList.add(new MergeRunnable(resultSet, recordByteWriter, agencyType));
                                }

                                executor.invokeAll(threadList);
                            }
                        }
                    } catch (SQLException | OpenAgencyException | InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            };

            return Response.ok(output).build();
        } finally {
            LOGGER.info("v1/dump");
        }
    }

    private AgencyType getAgencyType(int agencyId) throws OpenAgencyException {
        if (Arrays.asList(870970, 970971, 870979, 190002, 190004).contains(agencyId)) {
            return AgencyType.DBC;
        }

        boolean useEnrichments = openAgency.getService().libraryRules().isAllowed(agencyId, LibraryRuleHandler.Rule.USE_ENRICHMENTS);

        // Yes, 191919 is a DBC agency, however when dumping the records they should be treated as a local record as
        // they are pure enrichment records and can't be merged unless the parent record is dumped
        if (!useEnrichments || 191919 == agencyId) {
            return AgencyType.LOCAL;
        } else {
            return AgencyType.FBS;
        }
    }

    private List<String> validateParams(Params params) {
        List<String> result = new ArrayList<>();
        boolean hasFBSLibrary = false;

        if (params.getAgencies() == null || params.getAgencies().size() == 0) {
            result.add("agencies: Der skal være mindst ét biblioteksnummer angivet");
        }

        for (Integer agencyId : params.getAgencies()) {
            if (agencyId.toString().length() != 6) {
                result.add("agencies: Bibliotek " + agencyId.toString() + " er ikke et gyldigt biblioteksnummer da tekststrengen ikke er 6 tegn langt");
            }

            if (agencyId == 191919 && params.getAgencies().size() > 1) {
                result.add("agencies: Kombination af 191919 og andre bibliotekter er ikke gyldig. Hvis du er *helt* sikker på at du vil dumpe 191919 (DBC påhængsposter) så skal det gøres en i kørsel med kun 191919 i agencies");
            }
        }

        for (int agencyId : params.getAgencies()) {
            try {
                AgencyType agencyType = getAgencyType(agencyId);

                if (agencyType == AgencyType.FBS) {
                    hasFBSLibrary = true;
                }
            } catch (OpenAgencyException e) {
                result.add("agencies: Biblioteksnummer '" + agencyId + "' blev ikke valideret af OpenAgency");
            }
        }

        if (params.getRecordStatus() == null) {
            params.setRecordStatus(RecordStatus.ACTIVE.toString()); // Set default value
        } else {
            try {
                RecordStatus.fromString(params.getRecordStatus());
            } catch (IllegalArgumentException e) {
                result.add("recordStatus: Værdien '" + params.getRecordStatus() + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + RecordStatus.validValues());
            }
        }

        if (params.getOutputFormat() == null) {
            params.setOutputFormat(OutputFormat.LINE.toString()); // Set default value
        } else {
            try {
                OutputFormat.fromString(params.getOutputFormat());
            } catch (IllegalArgumentException e) {
                result.add("outputFormat: Værdien '" + params.getOutputFormat() + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + OutputFormat.validValues());
            }
        }

        if (params.getRecordType() != null) { // Validate values if present
            if (params.getRecordType().size() == 0) {
                result.add("recordType: Hvis feltet er med, så skal listen indeholde mindst én af følgende værdier: " + RecordType.validValues());
            } else {
                for (String recordType : params.getRecordType()) {
                    try {
                        RecordType.fromString(recordType);
                    } catch (IllegalArgumentException e) {
                        result.add("recordType: Værdien '" + recordType + "' er ikke en gyldig værdi. Feltet skal have en af følgende værdier: " + RecordType.validValues());
                    }
                }
            }
        } else { // Validate if recordType is allowed to not be present
            if (hasFBSLibrary) {
                result.add("recordType: Da der findes mindst ét FBS bibliotek i agencies er dette felt påkrævet.");
            }
        }

        if (params.getOutputEncoding() == null) {
            params.setOutputEncoding("UTF-8"); // Set default value
        } else {
            try {
                Charset.forName(params.getOutputEncoding());
            } catch (UnsupportedCharsetException ex) {
                result.add("outputEncoding: Værdien '" + params.getOutputEncoding() + "' er ikke et gyldigt charset");
            }
        }

        if (params.getCreated() != null) {
            if (params.getCreated().getFrom() != null) {
                try {
                    Date.valueOf(params.getCreated().getFrom());
                } catch (IllegalArgumentException e) {
                    result.add("created: Værdien i 'from' har ikke et datoformat som kan fortolkes");
                }
            }

            if (params.getCreated().getTo() != null) {
                try {
                    Date.valueOf(params.getCreated().getTo());
                } catch (IllegalArgumentException e) {
                    result.add("created: Værdien i 'to' har ikke et datoformat som kan fortolkes");
                }
            }
        }

        if (params.getModified() != null) {
            if (params.getModified().getFrom() != null) {
                try {
                    Date.valueOf(params.getModified().getFrom());
                } catch (IllegalArgumentException e) {
                    result.add("modified: Værdien i 'from' har ikke et datoformat som kan fortolkes");
                }
            }

            if (params.getModified().getTo() != null) {
                try {
                    Date.valueOf(params.getModified().getTo());
                } catch (IllegalArgumentException e) {
                    result.add("modified: Værdien i 'to' har ikke et datoformat som kan fortolkes");
                }
            }
        }

        return result;
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
