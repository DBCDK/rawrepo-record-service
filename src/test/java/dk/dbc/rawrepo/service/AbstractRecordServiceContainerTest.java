/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpClient;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.openagency.client.OpenAgencyServiceFromURL;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsOpenAgency;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

class AbstractRecordServiceContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRecordServiceContainerTest.class);

    static final String MIMETYPE_ARTICLE = "text/article+marcxchange";
    static final String MIMETYPE_ENRICHMENT = "text/enrichment+marcxchange";
    static final String MIMETYPE_LITANALYSIS = "text/litanalysis+marcxchange";
    static final String MIMETYPE_AUTHORITY = "text/authority+marcxchange";
    static final String MIMETYPE_MARCXCHANGE = "text/marcxchange";

    private static final GenericContainer recordServiceContainer;
    private static final GenericContainer rawrepoDbContainer;
    private static final GenericContainer holdingsItemsDbContainer;

    private static final RelationHintsOpenAgency relationHints;
    private static final OpenAgencyServiceFromURL openAgency;

    private static final String rawrepoDbBaseUrl;
    private static final String holdingsItemsDbUrl;
    static final String recordServiceBaseUrl;
    static final HttpClient httpClient;

    static {
        openAgency = OpenAgencyServiceFromURL.builder().build("http://openagency.addi.dk/2.34/");

        relationHints = new RelationHintsOpenAgency(openAgency);

        Network network = Network.newNetwork();

        rawrepoDbContainer = new GenericContainer("docker-io.dbc.dk/rawrepo-postgres-1.13-snapshot:DIT-5016")
                .withNetwork(network)
                .withNetworkAliases("rawrepoDb")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("POSTGRES_DB", "rawrepo")
                .withEnv("POSTGRES_USER", "rawrepo")
                .withEnv("POSTGRES_PASSWORD", "rawrepo")
                .withExposedPorts(5432)
                .withStartupTimeout(Duration.ofMinutes(1));
        rawrepoDbContainer.start();
        rawrepoDbBaseUrl = "rawrepo:rawrepo@rawrepoDb:5432/rawrepo";

        holdingsItemsDbContainer = new GenericContainer("docker-os.dbc.dk/holdings-items-postgres-1.1.4:latest")
                .withNetwork(network)
                .withNetworkAliases("holdingsItemsDb")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("POSTGRES_DB", "holdings")
                .withEnv("POSTGRES_USER", "holdings")
                .withEnv("POSTGRES_PASSWORD", "holdings")
                .withExposedPorts(5432)
                .withStartupTimeout(Duration.ofMinutes(1));
        holdingsItemsDbContainer.start();
        holdingsItemsDbUrl = "holdings:holdings@holdingsItemsDb:5432/holdings";

        recordServiceContainer = new GenericContainer("docker-io.dbc.dk/rawrepo-record-service:devel")
                .withNetwork(network)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("INSTANCE", "it")
                .withEnv("LOG_FORMAT", "text")
                .withEnv("OPENAGENCY_CACHE_AGE", "0")
                .withEnv("OPENAGENCY_URL", "http://openagency.addi.dk/2.34/")
                .withEnv("RAWREPO_URL", rawrepoDbBaseUrl)
                .withEnv("HOLDINGS_URL", holdingsItemsDbUrl)
                .withEnv("DUMP_THREAD_COUNT", "8")
                .withEnv("DUMP_SLIZE_SIZE", "1000")
                .withEnv("JAVA_MAX_HEAP_SIZE", "2G")
                .withExposedPorts(8080)
                .waitingFor(Wait.forHttp("/api/status"))
                .withStartupTimeout(Duration.ofMinutes(5));
        recordServiceContainer.start();
        recordServiceBaseUrl = "http://" + recordServiceContainer.getContainerIpAddress() +
                ":" + recordServiceContainer.getMappedPort(8080);
        httpClient = HttpClient.create(HttpClient.newClient());
    }

    private static RawRepoDAO createDAO(Connection conn) throws RawRepoException {
        final RawRepoDAO.Builder rawRepoBuilder = RawRepoDAO.builder(conn);
        rawRepoBuilder.relationHints(relationHints);
        return rawRepoBuilder.build();
    }

    static MarcRecord getMarcRecordFromFile(String fileName) throws MarcReaderException {
        final InputStream inputStream = AbstractRecordServiceContainerTest.class.getResourceAsStream(fileName);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        return reader.read();
    }

    static MarcRecord getMarcRecordFromString(byte[] content) throws MarcReaderException {
        final InputStream inputStream = new ByteArrayInputStream(content);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        return reader.read();
    }

    static HashMap<String, MarcRecord> getMarcRecordCollectionFromFile(String fileName) throws MarcReaderException {
        final InputStream inputStream = AbstractRecordServiceContainerTest.class.getResourceAsStream(fileName);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        final HashMap<String, MarcRecord> collection = new HashMap<>();

        MarcRecord marcRecord = reader.read();
        while (marcRecord != null) {
            collection.put(marcRecord.getSubFieldValue("001", 'a').get(), marcRecord);
            marcRecord = reader.read();
        }

        return collection;
    }

    static HashMap<String, MarcRecord> getMarcRecordCollectionFromString(String content) throws MarcReaderException {
        final InputStream inputStream = new ByteArrayInputStream(content.getBytes());
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final MarcXchangeV1Reader reader = new MarcXchangeV1Reader(bufferedInputStream, StandardCharsets.UTF_8);

        final HashMap<String, MarcRecord> collection = new HashMap<>();

        MarcRecord marcRecord = reader.read();
        while (marcRecord != null) {
            collection.put(marcRecord.getSubFieldValue("001", 'a').get(), marcRecord);
            marcRecord = reader.read();
        }

        return collection;
    }

    static void saveRecord(Connection connection, String fileName, String mimeType) throws Exception {
        final RawRepoDAO dao = createDAO(connection);
        final MarcXchangeV1Writer writer = new MarcXchangeV1Writer();

        final MarcRecord marcRecord = getMarcRecordFromFile(fileName);

        final String bibliographicRecordId = marcRecord.getSubFieldValue("001", 'a').get();
        final int agencyId = Integer.parseInt(marcRecord.getSubFieldValue("001", 'b').get());
        final boolean deleted = "d".equalsIgnoreCase(marcRecord.getSubFieldValue("004", 'r').get());
        final byte[] content = writer.write(marcRecord, StandardCharsets.UTF_8);
        final String trackingId = "";

        final Record record = dao.fetchRecord(bibliographicRecordId, agencyId);
        record.setDeleted(deleted);
        record.setMimeType(mimeType);
        record.setContent(content);
        record.setCreated(Instant.now());
        record.setModified(Instant.now());
        record.setTrackingId(trackingId);

        dao.saveRecord(record);
    }

    static void saveRelations(Connection connection, String bibliographicRecordId, int agencyId, String referBibliographicRecordId, int referAgencyId) throws Exception {
        final RawRepoDAO dao = createDAO(connection);
        final RecordId from = new RecordId(bibliographicRecordId, agencyId);
        final RecordId to = new RecordId(referBibliographicRecordId, referAgencyId);

        dao.setRelationsFrom(from, new HashSet<>(Collections.singletonList(to)));
    }

    static void resetRawrepoDb(Connection connection) throws Exception {
        final List<String> tables = Arrays.asList("relations", "records", "records_cache", "records_archive", "queue", "jobdiag");

        PreparedStatement stmt;

        for (String table : tables) {
            stmt = connection.prepareStatement("TRUNCATE " + table + " CASCADE");
            stmt.execute();
        }
    }

    static QueueJob dequeue(Connection connection, String worker) throws Exception {
        final RawRepoDAO dao = createDAO(connection);

        return dao.dequeue(worker);
    }

    static void resetRawrepoQueue(Connection connection) throws Exception {
        final PreparedStatement stmt = connection.prepareStatement("TRUNCATE queue CASCADE");
        stmt.execute();
    }

    static Connection connectToRawrepoDb() {
        try {
            Class.forName("org.postgresql.Driver");
            final String dbUrl = String.format("jdbc:postgresql://localhost:%s/rawrepo", rawrepoDbContainer.getMappedPort(5432));
            final Connection connection = DriverManager.getConnection(dbUrl, "rawrepo", "rawrepo");
            connection.setAutoCommit(true);

            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static Connection connectToHoldingsItemsDb() {
        try {
            Class.forName("org.postgresql.Driver");
            final String dbUrl = String.format("jdbc:postgresql://localhost:%s/holdings", holdingsItemsDbContainer.getMappedPort(5432));
            final Connection connection = DriverManager.getConnection(dbUrl, "holdings", "holdings");
            connection.setAutoCommit(true);

            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}