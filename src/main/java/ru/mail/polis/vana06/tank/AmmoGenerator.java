package ru.mail.polis.vana06.tank;

import org.jetbrains.annotations.NotNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class AmmoGenerator {
    private static final int VALUE_LENGTH = 256;
    private static long SEED = System.currentTimeMillis();
    private static Random random = new Random(SEED);
    private static int ACK = 2;
    private static final int FROM = 3;

    @NotNull
    private static String randomKey() {
        return Long.toHexString(random.nextLong());
    }

    @NotNull
    private static byte[] randomValue() {
        final byte[] result = new byte[VALUE_LENGTH];
        ThreadLocalRandom.current().nextBytes(result);
        return result;
    }

    private static void put() throws IOException {
        final String key = randomKey();
        final byte[] value = randomValue();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request)) {
            writer.write("PUT /v0/entity?id=" + key + " HTTP/1.1\r\n");
            writer.write("Content-Length: " + value.length + "\r\n");
            writer.write("\r\n");
        }
        request.write(value);
        System.out.write(Integer.valueOf(request.size()).toString().getBytes());
        System.out.write(" put\n".getBytes());
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes());
    }

    private static void get() throws IOException {
        final String key = randomKey();
        final ByteArrayOutputStream request = new ByteArrayOutputStream();
        try (Writer writer = new OutputStreamWriter(request)) {
            writer.write("GET /v0/entity?id=" + key + "&replicas=" + ACK + "/" + FROM + " HTTP/1.1\r\n");
            writer.write("\r\n");
        }
        System.out.write(Integer.valueOf(request.size()).toString().getBytes());
        System.out.write(" get\n".getBytes());
        request.writeTo(System.out);
        System.out.write("\r\n".getBytes());
    }

    public static void main(String[] args) throws IOException {
        if (args.length < 4 || args.length > 5) {
            System.err.println("Usage:\n\tjava -jar ... <put|get|put+get|get+put> <requests> <repeat key percent> <ack> [<seed>]");
            System.exit(-1);
        }

        final String mode = args[0];
        final int requests = Integer.parseInt(args[1]);

        final int percent = Integer.parseInt(args[2]);
        if (percent < 0 || percent > 50) {
            System.err.println("Repeat percent must be within [0;50]");
            System.exit(-1);
        }
        int requestsWithoutRepeat = (int) (requests * (100 - percent) / 100.0f);
        int requestsWithRepeat = requests - requestsWithoutRepeat;

        ACK = Integer.parseInt(args[3]);
        if(ACK > FROM || ACK <= 0){
            System.err.println("ack must be greater than 0 and less than from = " + FROM);
            System.exit(-1);
        }

        if(args.length == 5){
            SEED = Long.parseLong(args[4]);
            random = new Random(SEED);
        }

        switch (mode) {
            case "put":
                for (int i = 0; i < requestsWithoutRepeat; i++) {
                    put();
                }
                random = new Random(SEED);
                for (int i = 0; i < requestsWithRepeat; i++) {
                    put();
                }
                break;
            case "get":
                for (int i = 0; i < requestsWithoutRepeat; i++) {
                    get();
                }
                random = new Random(SEED);
                for (int i = 0; i < requestsWithRepeat; i++) {
                    get();
                }
                break;
            case "put+get":
            case "get+put":
                for (int i = 0; i < requestsWithoutRepeat / 2; i++) {
                    put();
                }
                random = new Random(SEED);
                for (int i = 0; i < requestsWithRepeat / 2; i++) {
                    put();
                }
                random = new Random(SEED);
                for (int i = 0; i < requestsWithoutRepeat / 2; i++) {
                    get();
                }
                random = new Random(SEED);
                for (int i = 0; i < requestsWithRepeat / 2; i++) {
                    get();
                }
                break;
            default:
                System.err.println("Unknown method");
        }
    }
}
