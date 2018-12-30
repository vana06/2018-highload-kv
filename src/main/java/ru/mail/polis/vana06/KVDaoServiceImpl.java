package ru.mail.polis.vana06;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.serial.Json;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.vana06.handler.DeleteHandler;
import ru.mail.polis.vana06.handler.GetHandler;
import ru.mail.polis.vana06.handler.PutHandler;
import ru.mail.polis.vana06.handler.RequestHandler;

import javax.xml.bind.DatatypeConverter;
import java.io.*;
import java.net.URL;
import java.nio.file.Files;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;

/**
 * Простой веб-сервер на основе one-nio
 *
 * @author Ivan Kylchik
 */
public class KVDaoServiceImpl extends HttpServer implements KVService {

    private final int port;
    @NotNull
    private final KVDaoImpl dao;
    @NotNull
    private final String[] topology;
    private final String me;
    private final RF defaultRF;
    private final Map<String, HttpClient> clients;

    private static final Logger log = LoggerFactory.getLogger(KVDaoServiceImpl.class);

    private final int THREAD_COUNT = 500;
    private final ExecutorService executor = Executors.newFixedThreadPool(THREAD_COUNT);

    public final static String PROXY_HEADER = "proxied";
    private final String STATUS_PATH = "/v0/status";
    public final static String ENTITY_PATH = "/v0/entity";

    /**
     * Инициализирует сервер на порту {@code port}
     *
     * @param port     порт для инициализации
     * @param dao      хранилище данных
     * @param topology топология всех кластеров
     * @throws IOException пробрасывает суперкласс {@code HttpServer}
     */
    public KVDaoServiceImpl(final int port,
                            @NotNull final KVDao dao,
                            @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.port = port;
        this.dao = (KVDaoImpl) dao;
        this.topology = topology.toArray(new String[0]);
        defaultRF = new RF(this.topology.length / 2 + 1, this.topology.length);
        clients = topology
                .stream()
                .filter(node -> !node.endsWith(String.valueOf(port)))
                .collect(Collectors.toMap(
                        o -> o,
                        o -> new HttpClient(new ConnectionString(o))));

        me = Arrays
                .stream(this.topology)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElseThrow(() -> {
                    IOException e = new IOException("Сервер на порту " + port + " не присутствует в топологии кластеров " + Arrays.toString(this.topology));
                    log.info(e.getMessage(), e);
                    return e;
                });

        log.info("Сервер на порту " + port + " был запущен");
    }

    private static HttpServerConfig create(int port) {
        AcceptorConfig ac = new AcceptorConfig();
        ac.port = port;

        HttpServerConfig config = new HttpServerConfig();
        config.acceptors = new AcceptorConfig[]{ac};
        return config;
    }

    /**
     * Метод возвращает текущий статус сервера
     *
     * @return текущий статус
     */
    @Path(STATUS_PATH)
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Метод обрабатывает реализованные в сервисе функции, такие как GET, PUT, DELETE
     *
     * @param request  данные запросы
     * @param session  реализует методы для ответа
     * @param id       - непустая последовательность символов, используется как ключ
     * @param replicas содержит количетство узлов, которые должны подтвердить операцию, чтобы она считалась выполненной успешно
     * @throws IOException пробрасывается методом {@code sendError}
     */
    @Path(ENTITY_PATH)
    public void handler(Request request,
                        HttpSession session,
                        @Param("id=") String id,
                        @Param("part=") String partStr,
                        @Param("replicas=") String replicas) throws IOException {
        log.info("Параметры запроса:\n" + request);

        if (id == null || id.isEmpty()) {
            log.error("id = \'" + id + "\' не удовлетворяет требованиям");
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        long part;
        if (partStr == null || partStr.isEmpty()) {
            part = 0;
        } else {
            part = Long.parseLong(partStr);
            if (part < 0) {
                log.error("part = \'" + partStr + "\' не удовлетворяет требованиям");
                session.sendError(Response.BAD_REQUEST, "Номер части должен быть больше либо равен 0");
                return;
            }
        }

        RF rf;
        if (replicas == null || replicas.isEmpty()) {
            rf = defaultRF;
        } else {
            try {
                rf = new RF(replicas);
            } catch (IllegalArgumentException e) {
                log.error(e.getMessage(), e);
                session.sendError(Response.BAD_REQUEST, null);
                return;
            }
        }

        String[] nodes;
        try {
            nodes = replicas(id, rf.getFrom());
        } catch (IllegalArgumentException e) {
            log.error(e.getMessage(), e);
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        boolean proxied = request.getHeader(PROXY_HEADER) != null;
        log.info("Тип запроса - " + getMethodName(request.getMethod()) + "; proxied - " + proxied);
        try {

            switch (request.getMethod()) {
                case Request.METHOD_GET:
                    session.sendResponse(customHandler(new GetHandler("GET", dao, rf, id, part), nodes, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(customHandler(new PutHandler("PUT", dao, rf, id, part, request.getBody()), nodes, proxied));
                    return;
                case Request.METHOD_DELETE:
                    byte[] metadata = dao.internalGet(id.getBytes(), 0).getData();
                    session.sendResponse(customHandler(new DeleteHandler("DELETE", dao, rf, id, 0L), nodes, proxied));

                    JSONObject obj = new JSONObject(new String(metadata));
                    long partQuantity = obj.getLong("chunkQuantity");
                    for (long i = 0; i < partQuantity; i++) {
                        customHandler(new DeleteHandler("DELETE", dao, rf, id, i), nodes, proxied);
                    }

                    return;
                default:
                    log.error(request.getMethod() + " неподдерживаемый код метод");
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            log.info("Элемент по ключу " + id + " не найден", e);
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e) {
            log.error("Внутренняя ошибка сервера", e);
            log.error("Параметры запроса:\n" + request);
            session.sendError(Response.INTERNAL_ERROR, null);
        } catch (JSONException e) {
            log.info("Объект с id=" + id + " хранится в виде 1 части");
            log.error(e.getMessage(), e);
        }
    }

    @Path("/v0/streaming")
    public void streaming(Request request,
                          HttpSession session,
                          @Param("id=") String id,
                          @Param("part=") String part,
                          @Param("replicas=") String replicas) throws IOException, JSONException {
        log.info("Параметры запроса на streaming:\n" + request);

        switch (request.getMethod()) {
            case Request.METHOD_GET:
                //возвращаю страницу
                if (id == null || id.isEmpty()) {
                    URL fileUrl = getClass().getClassLoader().getResource("streaming.html");
                    if (fileUrl != null) {
                        byte[] fileContent = Files.readAllBytes(new File(fileUrl.getPath()).toPath());
                        Response response = Response.ok(fileContent);
                        response.addHeader("Content-Type: text/html");

                        session.sendResponse(response);
                    } else {
                        session.sendError(Response.NOT_FOUND, null);
                    }
                } else {
                    if (part.equals("0")) {
                        handler(request, session, id, part, replicas);
                    } else {
                        handler(request, session, id, part, replicas);
                    }
                }
                break;
            case Request.METHOD_PUT:
                log.debug("PUT");
                if (part.equals("0")) {
                    //начинаю преготовления к сохранению данных
                    JSONObject json = new JSONObject(new String(request.getBody()));
                    if (!json.has("fileName") || !json.has("chunkQuantity")) {
                        session.sendError(Response.BAD_REQUEST, "Json должен сожержать поля fileName и chunkQuantity");
                        return;
                    }

                    String fileName = json.getString("fileName");
                    if (!json.has("type")) {
                        if (fileName.endsWith("webm")) {
                            json.put("type", "video");
                        } else if (fileName.endsWith("mp3")) {
                            json.put("type", "audio");
                        } else {
                            json.put("type", "text");
                        }
                        request.setBody(json.toString().getBytes());
                    }
                    handler(request, session, json.getString("fileName"), part, replicas);
                } else {
                    log.debug("PART = " + part);
                    handler(request, session, id, part, replicas);
                }

                break;
            default:
                log.error(request.getMethod() + " неподдерживаемый код метод");
                session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
        }
    }

    /**
     * Обрабатывает все неконтроллируемые/нереализованные запросы
     *
     * @param request данные запросы
     * @param session реализует методы для ответа
     * @throws IOException пробрасывается методом {@code sendError}
     */
    @Override
    public void handleDefault(Request request, HttpSession session) throws IOException {
        log.error("Неподдерживаемый запрос\n" + request);
        session.sendError(Response.BAD_REQUEST, null);
    }

    private Response customHandler(RequestHandler rh, String[] nodes, boolean proxied) throws IOException, NoSuchElementException {
        if (proxied) {
            return rh.onProxied();
        }

        ArrayList<Future<Boolean>> futures = new ArrayList<>();
        for (final String node : nodes) {
            if (node.equals(me)) {
                futures.add(executor.submit(rh.ifMe()));
            } else {
                futures.add(executor.submit(rh.ifNotMe(clients.get(node))));
            }
        }

        return rh.getResponse(futures);
    }

    /**
     * Формирует ноды для работы
     *
     * @param id    ключ
     * @param count количество нод
     * @return список используемых нод
     * @throws IllegalArgumentException в случае, когда count больше числа доступных нод
     */
    private String[] replicas(String id, int count) throws IllegalArgumentException {
        if (count > topology.length) {
            throw new IllegalArgumentException("The from value must be less or equal to the total count of nodes = " + topology.length);
        }
        String[] result = new String[count];
        int i = (id.hashCode() & Integer.MAX_VALUE) % topology.length;
        for (int j = 0; j < count; j++) {
            result[j] = topology[i];
            i = (i + 1) % topology.length;
        }
        return result;
    }

    private String getMethodName(int method) {
        switch (method) {
            case Request.METHOD_GET:
                return "GET";
            case Request.METHOD_PUT:
                return "PUT";
            case Request.METHOD_DELETE:
                return "DELETE";
            default:
                return "UNSUPPORTED " + method;
        }
    }

    @Override
    public synchronized void stop() {
        super.stop();
        executor.shutdown();
        log.info("Сервер на порту " + port + " был остановлен");
    }
}
