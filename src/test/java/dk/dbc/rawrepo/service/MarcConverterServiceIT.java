package dk.dbc.rawrepo.service;

import dk.dbc.httpclient.HttpPost;
import dk.dbc.httpclient.PathBuilder;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class MarcConverterServiceIT extends AbstractRecordServiceContainerTest {

    static Stream<Arguments> provideConversionResults() {
        return Stream.of(
                arguments("mconv/worldcat_dm2_collection.xml", "mconv/worldcat_dm2_collection.json", "JSONL", "application/json"),
                arguments("mconv/62451157.xml", "mconv/62451157.txt", "LINE", "text/plain"),
                arguments("mconv/62451157.txt", "mconv/62451157.xml", "MARCXCHANGE", "application/xml")
        );
    }

    @ParameterizedTest
    @MethodSource("provideConversionResults")
    void testConversions(String inputFilename, String expectedFilename, String outputFormat, String expectedContentType) {
        final String input = loadFile(inputFilename);
        final String expected = loadFile(expectedFilename);

        final PathBuilder path = new PathBuilder("/api/v1/mconv");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withData(input, MediaType.APPLICATION_XML)
                .withPathElements(path.build());

        final HashMap<String, Object> params = new HashMap<>();
        params.put("output-format", outputFormat);

        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }

        try (Response response = httpClient.execute(httpPost)) {
            assertThat("Response code", response.getStatus(), is(200));
            // Trim is to ignore trailing newline
            assertThat("Content", response.readEntity(String.class).trim(), is(expected.trim()));
            assertThat("Content-type", response.getStringHeaders().get("Content-Type").get(0), is(expectedContentType));
        }
    }

    static Stream<Arguments> provideInvalidParamValues() {
        return Stream.of(
                arguments("output-format", "No enum constant dk.dbc.marc.RecordFormat.julemand"),
                arguments("input-encoding", "julemand is not a valid charset"),
                arguments("output-encoding", "julemand is not a valid charset")
        );
    }

    @ParameterizedTest
    @MethodSource("provideInvalidParamValues")
    void testInvalidOutputFormat(String paramName, String expectedMessage) {
        final String input = loadFile("mconv/62451157.xml");

        final PathBuilder path = new PathBuilder("/api/v1/mconv");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withData(input, MediaType.APPLICATION_XML)
                .withPathElements(path.build());
        final HashMap<String, Object> params = new HashMap<>();
        params.put(paramName, "julemand");

        for (Map.Entry<String, Object> param : params.entrySet()) {
            httpPost.withQueryParameter(param.getKey(), param.getValue());
        }
        try (Response response = httpClient.execute(httpPost)) {
            assertThat("Response code", response.getStatus(), is(400));
            assertThat("Message", response.readEntity(String.class), is(expectedMessage));
        }
    }

    @Test
    void testMissingOutputFormat() {
        final String input = loadFile("mconv/62451157.xml");

        final PathBuilder path = new PathBuilder("/api/v1/mconv");
        final HttpPost httpPost = new HttpPost(httpClient)
                .withBaseUrl(recordServiceBaseUrl)
                .withData(input, MediaType.APPLICATION_XML)
                .withPathElements(path.build());
        try (Response response = httpClient.execute(httpPost)) {
            assertThat("Response code", response.getStatus(), is(400));
            assertThat("Message", response.readEntity(String.class), is("output-format must be set"));
        }
    }

    private String loadFile(String filename) {
        final InputStream inputStream = AbstractRecordServiceContainerTest.class.getResourceAsStream(filename);

        assert inputStream != null;
        return new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))
                .lines()
                .collect(Collectors.joining("\n"));
    }

}
