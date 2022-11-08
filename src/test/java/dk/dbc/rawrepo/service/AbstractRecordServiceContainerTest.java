package dk.dbc.rawrepo.service;

import dk.dbc.commons.testcontainers.postgres.DBCPostgreSQLContainer;
import dk.dbc.httpclient.HttpClient;
import dk.dbc.marc.binding.MarcRecord;
import dk.dbc.marc.reader.MarcReaderException;
import dk.dbc.marc.reader.MarcXchangeV1Reader;
import dk.dbc.marc.writer.MarcXchangeV1Writer;
import dk.dbc.rawrepo.QueueJob;
import dk.dbc.rawrepo.RawRepoDAO;
import dk.dbc.rawrepo.RawRepoException;
import dk.dbc.rawrepo.Record;
import dk.dbc.rawrepo.RecordId;
import dk.dbc.rawrepo.RelationHintsVipCore;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnector;
import dk.dbc.vipcore.libraryrules.VipCoreLibraryRulesConnectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
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
    static final String MIMETYPE_MATVURD = "text/matvurd+marcxchange";

    private static final GenericContainer recordServiceContainer;
    private static final DBCPostgreSQLContainer rawrepoDbContainer;
    private static final DBCPostgreSQLContainer holdingsItemsDbContainer;

    private static final VipCoreLibraryRulesConnector vipCoreLibraryRulesConnector;
    private static final RelationHintsVipCore relationHints;

    static final String recordServiceBaseUrl;
    static final HttpClient httpClient;

    static {
        vipCoreLibraryRulesConnector = VipCoreLibraryRulesConnectorFactory.create("http://vipcore.iscrum-vip-extern-test.svc.cloud.dbc.dk");

        relationHints = new RelationHintsVipCore(vipCoreLibraryRulesConnector);

        Network network = Network.newNetwork();

        rawrepoDbContainer = new DBCPostgreSQLContainer("docker-metascrum.artifacts.dbccloud.dk/rawrepo-postgres-1.16-snapshot:master-5178")
                .withDatabaseName("rawrepo")
                .withUsername("rawrepo")
                .withPassword("rawrepo");
        rawrepoDbContainer.start();
        rawrepoDbContainer.exposeHostPort();

        holdingsItemsDbContainer = new DBCPostgreSQLContainer("docker-os.dbc.dk/holdings-items-postgres-1.1.4:latest")
                .withDatabaseName("holdings")
                .withUsername("holdings")
                .withPassword("holdings");
        holdingsItemsDbContainer.start();
        holdingsItemsDbContainer.exposeHostPort();

        recordServiceContainer = new GenericContainer("docker-metascrum.artifacts.dbccloud.dk/rawrepo-record-service:devel")
                .withNetwork(network)
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("INSTANCE", "it")
                .withEnv("LOG_FORMAT", "text")
                .withEnv("VIPCORE_CACHE_AGE", "1")
                .withEnv("VIPCORE_ENDPOINT", "http://vipcore.iscrum-vip-extern-test.svc.cloud.dbc.dk")
                .withEnv("RAWREPO_URL", rawrepoDbContainer.getPayaraDockerJdbcUrl())
                .withEnv("HOLDINGS_URL", holdingsItemsDbContainer.getPayaraDockerJdbcUrl())
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

    static byte[] getContentFromFile(String fileName) throws IOException {
        final InputStream inputStream = AbstractRecordServiceContainerTest.class.getResourceAsStream(fileName);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final int numByte = bufferedInputStream.available();
        final byte[] buf = new byte[numByte];
        bufferedInputStream.read(buf, 0, numByte);

        return buf;
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
        saveRecord(connection, fileName, mimeType, null, null);
    }

    static void saveRecord(Connection connection, String fileName, String mimeType, String created, String modified) throws Exception {
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
        if (created == null) {
            record.setCreated(Instant.now());
        } else {
            record.setCreated(Instant.parse(created));
        }
        if (modified == null) {
            record.setModified(Instant.now());
        } else {
            record.setModified(Instant.parse(modified));
        }
        record.setTrackingId(trackingId);

        dao.saveRecord(record);
    }

    static void saveRelations(Connection connection, String bibliographicRecordId, int agencyId, String referBibliographicRecordId, int referAgencyId) throws Exception {
        final RawRepoDAO dao = createDAO(connection);
        final RecordId from = new RecordId(bibliographicRecordId, agencyId);
        final RecordId to = new RecordId(referBibliographicRecordId, referAgencyId);

        dao.setRelationsFrom(from, new HashSet<>(Collections.singletonList(to)));
    }

    static void saveRelations(Connection connection, String bibliographicRecordId, int agencyId, List<RecordId> to) throws Exception {
        final RawRepoDAO dao = createDAO(connection);
        final RecordId from = new RecordId(bibliographicRecordId, agencyId);

        dao.setRelationsFrom(from, new HashSet<>(to));
    }

    static void resetRawrepoDb(Connection connection) throws Exception {
        final List<String> tables = Arrays.asList("relations", "records", "records_archive", "queue", "jobdiag");

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
