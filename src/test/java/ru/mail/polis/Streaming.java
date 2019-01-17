package ru.mail.polis;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import one.nio.net.ConnectionString;
import org.jetbrains.annotations.NotNull;
import org.json.JSONObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class Streaming extends TestBase {
    private static final Duration TIMEOUT = Duration.ofMinutes(1);
    private static File data;
    private static KVDao dao;
    private static int port;
    private static String endpoint;
    private static KVService storage;
    private static HttpClient client;

    private static int chunkSize = 1024;

    @BeforeAll
    static void beforeAll() throws Exception {
        port = randomPort();
        data = Files.createTempDirectory();
        dao = KVDaoFactory.create(data);
        endpoint = endpoint(port);
        storage = KVServiceFactory.create(port, dao, Collections.singleton(endpoint));
        storage.start();
        Thread.sleep(TimeUnit.SECONDS.toMillis(1));
        reset();
    }

    @AfterAll
    static void afterAll() throws IOException {
        client.close();
        storage.stop();
        dao.close();
        Files.recursiveDelete(data);
    }

    private static void reset() {
        if (client != null) {
            client.close();
        }
        client = new HttpClient(
                new ConnectionString(
                        "http://localhost:" + port +
                                "?timeout=" + (TIMEOUT.toMillis() / 2)));
    }

    private Response get(@NotNull final String path, String... headers) throws Exception {
        return client.get(path, headers);
    }

    private Response upsert(
            @NotNull final String path,
            @NotNull final byte[] data) throws Exception {
        return client.put(path, data);
    }

    private byte[] generateBytes(int sizeInBytes) {
        byte[] byteArray = new byte[sizeInBytes];
        new Random().nextBytes(byteArray);
        return byteArray;
    }

    private byte[][] upsertStream(String fileName, int chunkQuantity) {
        byte[][] bytes = new byte[chunkQuantity][];
        for (int i = 0; i < bytes.length; i++) {
            bytes[i] = generateBytes(chunkSize);
        }

        Map<String, String> jsonMap = new HashMap<>();
        jsonMap.put("fileName", fileName);
        jsonMap.put("chunkQuantity", String.valueOf(chunkQuantity));
        jsonMap.put("fileSize", String.valueOf(chunkQuantity*chunkSize));
        JSONObject json = new JSONObject(jsonMap);

        assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                201,
                upsert("/v0/streaming?bytes=-1", json.toString().getBytes()).getStatus())
        );

        for (int i = 0; i < bytes.length; i++){
            String path = "/v0/streaming?id=" + fileName + "&bytes=" + (i*chunkSize);
            byte[] toSend = bytes[i];
            assertTimeoutPreemptively(TIMEOUT, () -> assertEquals(
                    201,
                    upsert(path, toSend).getStatus())
            );
        }

        return bytes;
    }

    private byte[][] getStream(String fileName) throws Exception {
        Response response = get("/v0/streaming?id=" + fileName + "&bytes=-1");
        assertEquals(200, response.getStatus());

        JSONObject json = new JSONObject(new String(response.getBody()));
        int fileSize = json.getInt("fileSize");
        int chunkQuantity = json.getInt("chunkQuantity");

        byte[][] bytes = new byte[chunkQuantity][];
        for (int i = 0; i < chunkQuantity; i++){
            response = get("/v0/streaming?id=" + fileName + "&size=" + fileSize, "Range: bytes=" + (i*chunkSize) + "-");

            assertEquals(206, response.getStatus());
            bytes[i] = response.getBody();
        }

        return bytes;
    }

    @Test
    void insertAndGet() throws Exception {
        final String fileName = "temp";
        byte[][] inserted = upsertStream(fileName, 10);
        byte[][] received = getStream(fileName);

        assertEquals(inserted.length, received.length);
        for (int i = 0; i < received.length; i++) {
            assertArrayEquals(inserted[i], received[i]);
        }
    }

}
