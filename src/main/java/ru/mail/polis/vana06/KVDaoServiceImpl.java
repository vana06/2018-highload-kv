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

    private final String PROXY_HEADER = "proxied";
    private final String TIMESTAMP = "timestamp";
    private final String STATE = "state";
    private final String STATUS_PATH = "/v0/status";
    private final String ENTITY_PATH = "/v0/entity";

    /**
     * Инициализирует сервер на порту {@code port}
     *
     * @param port порт для инициализации
     * @param dao хранилище данных
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
                .orElse(null);

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
     * @param request данные запросы
     * @param session реализует методы для ответа
     * @param id - непустая последовательность символов, используется как ключ
     * @param replicas содержит количетство узлов, которые должны подтвердить операцию, чтобы она считалась выполненной успешно
     * @throws IOException пробрасывается методом {@code sendError}
     */
    @Path(ENTITY_PATH)
    public void handler(Request request,
                        HttpSession session,
                        @Param("id=") String id,
                        @Param("replicas=") String replicas) throws IOException{
        log.info("Запрос от " + request.getHost() + "; URI = " + request.getURI());

        if(id == null || id.isEmpty()){
            log.error("id = " + id + " не удовлетворяет требованиям");
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        RF rf;
        if(replicas == null || replicas.isEmpty()){
            rf = defaultRF;
        } else {
            try {
                rf = new RF(replicas);
            } catch (IllegalArgumentException e){
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
            switch (request.getMethod()){
                case Request.METHOD_GET:
                    session.sendResponse(get(id, nodes, rf, proxied));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(put(id, request.getBody(), nodes, rf, proxied));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(delete(id, nodes, rf, proxied));
                    return;
                default:
                    log.error(request.getMethod() + " неподдерживаемый код метод");
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            log.info("Элемент по ключу " + id + " не найден", e);
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e){
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

    /**
     * Обработка GET запроса
     *
     * @param id ключ
     * @param nodes доступные ноды
     * @param rf текущая replica factor
     * @param proxied является ли текущая нода прокси сервером
     * @return
     * <ol>
     * <li> 200 OK и данные, если ответили хотя бы ack из from реплик </li>
     * <li> 404 Not Found, если ни одна из ack реплик, вернувших ответ, не содержит данные (либо данные удалены хотя бы на одной из ack ответивших реплик) </li>
     * <li> 504 Not Enough Replicas, если не получили 200/404 от ack реплик из всего множества from реплик </li>
     * </ol>
     * @throws IOException в случае внутренних ошибок на сервере
     * @throws NoSuchElementException если элемент не был найден по ключу
     */
    private Response get(String id, String[] nodes, RF rf, boolean proxied) throws IOException, NoSuchElementException{
        if(proxied){
            Value value = dao.internalGet(id.getBytes());
            if(value.getState() == Value.State.PRESENT || value.getState() == Value.State.ABSENT){
                Response response = new Response(Response.OK, value.getData());
                response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
                response.addHeader(STATE + value.getState().name());
                return response;
            } else if(value.getState() == Value.State.REMOVED){
                throw new NoSuchElementException();
            }
        }

        List<Value> values = new ArrayList<>();

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                Value value = dao.internalGet(id.getBytes());
                if(value.getState() == Value.State.PRESENT || value.getState() == Value.State.ABSENT){
                    acks++;
                    values.add(value);
                } else if(value.getState() == Value.State.REMOVED){
                    throw new NoSuchElementException();
                }
            } else {
                try {
                    final Response response = clients.get(node).get(ENTITY_PATH + "?id=" + id, PROXY_HEADER);

                    if(response.getStatus() == 200){
                        values.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)),
                                Value.State.valueOf(response.getHeader(STATE))));

                        acks++;
                    } else if(response.getStatus() == 404){
                        throw new NoSuchElementException();
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        if(acks >= rf.getAck()){
            if(values.stream().anyMatch(value -> value.getState() == Value.State.PRESENT)){
                Value max = values
                        .stream()
                        .max(Comparator.comparing(Value::getTimestamp))
                        .get();
                return success("GET", Response.OK, acks, rf, max.getData());
            } else {
                throw new NoSuchElementException();
            }
        } else {
            return gatewayTimeout("GET", acks, rf);
        }
    }

    /**
     * Обработка PUT запроса
     *
     * @param id ключ
     * @param value значение
     * @param nodes доступные ноды
     * @param rf текущая replica factor
     * @param proxied является ли текущая нода прокси сервером
     * @return
     * <ol>
     * <li> 201 Created, если хотя бы ack из from реплик подтвердили операцию </li>
     * <li> 504 Not Enough Replicas, если не набралось ack подтверждений из всего множества from реплик </li>
     * </ol>
     * @throws IOException в случае внутренних ошибок на сервере
     */
    private Response put(String id, byte[] value, String[] nodes, RF rf, boolean proxied) throws IOException {
        if(proxied){
            dao.upsert(id.getBytes(), value);
            return new Response(Response.CREATED, Response.EMPTY);
        }

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                dao.upsert(id.getBytes(), value);
                acks++;
            } else {
                try {
                    final Response response = clients.get(node).put(ENTITY_PATH + "?id=" + id, value, PROXY_HEADER);
                    if(response.getStatus() == 201){
                        acks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        if(acks >= rf.getAck()){
            return success("PUT", Response.CREATED, acks, rf, Response.EMPTY);
        } else {
            return gatewayTimeout("PUT", acks, rf);
        }
    }

    /**
     * Обработка DELETE запроса
     *
     * @param id ключ
     * @param nodes доступные ноды
     * @param rf текущая replica factor
     * @param proxied является ли текущая нода прокси сервером
     * @return
     * <ol>
     * <li> 202 Accepted, если хотя бы ack из from реплик подтвердили операцию </li>
     * <li> 504 Not Enough Replicas, если не набралось ack подтверждений из всего множества from реплик </li>
     * </ol>
     * @throws IOException в случае внутренних ошибок на сервере
     */
    private Response delete(String id, String[] nodes, RF rf, boolean proxied) throws IOException {
        if(proxied){
            dao.remove(id.getBytes());
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                dao.remove(id.getBytes());
                acks++;
            } else {
                try {
                    final Response response = clients.get(node).delete(ENTITY_PATH + "?id=" + id, PROXY_HEADER);
                    if(response.getStatus() == 202){
                        acks++;
                    }
                } catch (InterruptedException | PoolException | HttpException e) {
                    log.error(e.getMessage(), e);
                }
            }
        }

        if(acks >= rf.getAck()){
            return success("DELETE", Response.ACCEPTED, acks, rf, Response.EMPTY);
        } else {
            return gatewayTimeout("DELETE", acks, rf);
        }

    }

    /**
     * Формирует ноды для работы
     *
     * @param id ключ
     * @param count количество нод
     * @return список используемых нод
     * @throws IllegalArgumentException в случае, когда count больше числа доступных нод
     */
    private String[] replicas(String id, int count) throws IllegalArgumentException{
        if(count > topology.length){
            throw new IllegalArgumentException("The from value must be less or equal to the total count of nodes = " + topology.length);
        }
        String[] result = new String[count];
        int i = (id.hashCode() & Integer.MAX_VALUE) % topology.length;
        for(int j = 0; j < count; j++){
            result[j] = topology[i];
            i = (i + 1) % topology.length;
        }
        return result;
    }

    private String getMethodName(int method){
        switch (method){
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

    private Response gatewayTimeout(String method, int acks, RF rf){
        log.info("Операция " + method + " не выполнена, acks = " + acks + " ; требования - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }
    private Response success(String method, String responseName, int acks, RF rf, byte[] body){
        log.info("Операция " + method + " выполнена успешно в " + acks + " нодах; требования - " + rf);
        return new Response(responseName, body);
    }

    @Override
    public synchronized void stop() {
        super.stop();
        log.info("Сервер на порту " + port + " был остановлен");
    }
}
