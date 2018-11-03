package ru.mail.polis.vana06;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;
import ru.mail.polis.vana06.handler.DeleteHandler;
import ru.mail.polis.vana06.handler.GetHandler;
import ru.mail.polis.vana06.handler.PutHandler;
import ru.mail.polis.vana06.handler.RequestHandler;

import java.io.*;
import java.util.*;
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
                        @Param("replicas=") String replicas) throws IOException {
        log.info("Запрос от " + request.getHost() + "; URI = " + request.getURI());

        if (id == null || id.isEmpty()) {
            log.error("id = " + id + " не удовлетворяет требованиям");
            session.sendError(Response.BAD_REQUEST, null);
            return;
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
                    session.sendResponse(customHandler(new GetHandler("GET", dao, rf, id), nodes, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(customHandler(new PutHandler("PUT", dao, rf, id, request.getBody()), nodes, proxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(customHandler(new DeleteHandler("DELETE", dao, rf, id), nodes, proxied));
                    return;
                default:
                    log.error(request.getMethod() + " неподдерживаемый код метод");
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            log.info("Элемент по ключу " + id + " не найден", e);
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e) {
            log.info("Внутренняя ошибка сервера", e);
            session.sendError(Response.INTERNAL_ERROR, null);
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
        log.error("Неподдерживаемый запрос от " + request.getHost() + "; URI = " + request.getURI());
        session.sendError(Response.BAD_REQUEST, null);
    }

    private Response customHandler(RequestHandler rh, String[] nodes, boolean proxied) throws IOException, NoSuchElementException {
        if (proxied) {
            return rh.onProxied();
        }

        int acks = 0;
        for (final String node : nodes) {
            if (node.equals(me)) {
                if (rh.ifMe()) {
                    acks++;
                }
            } else {
                try {
                    if (rh.ifNotMe(clients.get(node))) {
                        acks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
        return rh.getResponse(acks);
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
        log.info("Сервер на порту " + port + " был остановлен");
    }
}
