/*
 * Copyright Dansk Bibliotekscenter a/s. Licensed under GNU GPL v3
 *  See license text at https://opensource.dbc.dk/licenses/gpl-3.0
 */

package dk.dbc.rawrepo;

import com.opentable.db.postgres.embedded.EmbeddedPostgres;
import dk.dbc.httpclient.HttpClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.containers.wait.strategy.Wait;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.stream.Stream;

class AbstractRecordServiceContainerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRecordServiceContainerTest.class);

    private static final EmbeddedPostgres PG_RAWREPO = pgStart();
    private static final EmbeddedPostgres PG_HOLDINGS = pgStart();

    static {
        Testcontainers.exposeHostPorts(PG_RAWREPO.getPort());
    }

    private static final GenericContainer recordServiceContainer;
    static final String recordServiceBaseUrl;
    static final HttpClient httpClient;

    static {
        recordServiceContainer = new GenericContainer("docker-io.dbc.dk/rawrepo-record-service:devel")
                .withLogConsumer(new Slf4jLogConsumer(LOGGER))
                .withEnv("INSTANCE", "it")
                .withEnv("LOG_FORMAT", "text")
                .withEnv("OPENAGENCY_CACHE_AGE", "0")
                .withEnv("OPENAGENCY_URL", "http://openagency.addi.dk/2.34/")
                .withEnv("RAWREPO_URL", String.format("postgres:@host.testcontainers.internal:%s/postgres",
                        PG_RAWREPO.getPort()))
                .withEnv("HOLDINGS_URL", String.format("postgres:@host.testcontainers.internal:%s/postgres",
                        PG_HOLDINGS.getPort()))
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

    private static EmbeddedPostgres pgStart() {
        try {
            return EmbeddedPostgres.start();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    String fileToContent(String filename) throws Exception {
        final InputStream inputStream = this.getClass().getResourceAsStream(filename);
        final BufferedInputStream bufferedInputStream = new BufferedInputStream(inputStream);
        final ByteArrayOutputStream buffer = new ByteArrayOutputStream();

        int nRead;
        byte[] data = new byte[16384];

        while ((nRead = bufferedInputStream.read(data, 0, data.length)) != -1) {
            buffer.write(data, 0, nRead);
        }

        return buffer.toString();
    }

    static void insertRecord(Connection connection, String bibliographicRecordId, int agencyId, String mimeType, byte[] content) throws SQLException {
        final PreparedStatement stmt = connection.prepareStatement(
                "INSERT INTO records(bibliographicrecordid, agencyid, deleted, mimetype, content, created, modified, trackingid) VALUES (?, ?, ?, ?, ?, ?, ?, ?)");

        int i = 1;
        stmt.setString(i++, bibliographicRecordId);
        stmt.setInt(i++, agencyId);
        stmt.setBoolean(i++, false);
        stmt.setString(i++, mimeType);
        stmt.setBytes(i++, content);
        stmt.setTimestamp(i++, new Timestamp(System.currentTimeMillis()));
        stmt.setTimestamp(i++, new Timestamp(System.currentTimeMillis()));
        stmt.setString(i, "");
        stmt.execute();
    }

    static Connection connectToRawrepoDb() {
        try {
            Class.forName("org.postgresql.Driver");
            final String dbUrl = String.format("jdbc:postgresql://localhost:%s/postgres", PG_RAWREPO.getPort());
            final Connection connection = DriverManager.getConnection(dbUrl, "postgres", "");
            connection.setAutoCommit(true);

            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }

    static Connection connectToHoldingsItemsDb() {
        try {
            Class.forName("org.postgresql.Driver");
            final String dbUrl = String.format("jdbc:postgresql://localhost:%s/postgres", PG_HOLDINGS.getPort());
            final Connection connection = DriverManager.getConnection(dbUrl, "postgres", "");
            connection.setAutoCommit(true);

            return connection;
        } catch (ClassNotFoundException | SQLException e) {
            throw new RuntimeException(e);
        }
    }
}