package ru.mail.polis.vana06;

import one.nio.http.*;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.StringTokenizer;

/**
 * Простой веб-сервер на основе one-nio
 *
 * @author Ivan Kylchik
 */
public class KVDaoServiceImpl extends HttpServer implements KVService {

    @NotNull
    private final KVDao dao;
    @NotNull
    private final Set<String> topology;

    private final RF defaultRF;

    /**
     * Инициализирует сервер на порту {@code port}
     *
     * @param port порт для инициализации
     * @param dao хранилище данных
     * @throws IOException пробрасывает суперкласс {@code HttpServer}
     */
    public KVDaoServiceImpl(final int port,
                            @NotNull final KVDao dao,
                            @NotNull final Set<String> topology) throws IOException {
        super(create(port));
        this.dao = dao;
        this.topology = topology;
        defaultRF = new RF(topology.size() / 2 + 1, topology.size());
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
    @Path("/v0/status")
    public Response status() {
        return Response.ok("OK");
    }

    /**
     * Метод обрабатывает реализованные в сервисе функции, такие как GET, PUT, DELETE
     *
     * @param request данные запросы
     * @param session реализует методы для ответа
     * @param id - непустая последовательность символов, используется как ключ
     * @throws IOException пробрасывается методом {@code sendError}
     */
    @Path("/v0/entity")
    public void handler(Request request, HttpSession session, @Param("id=") String id, @Param("replicas=") String replicas) throws IOException{
        if(id == null || id.isEmpty()){
            session.sendError(Response.BAD_REQUEST, null);
            return;
        }

        RF  rf;
        if(replicas == null || replicas.isEmpty()){
            rf = defaultRF;
        } else {
            try {
                rf = new RF(replicas);
            } catch (IllegalArgumentException e){
                session.sendError(Response.BAD_REQUEST, e.getMessage());
            }
        }

        try {
            switch (request.getMethod()){
                case Request.METHOD_GET:
                    session.sendResponse(get(id));
                    return;
                case Request.METHOD_PUT:
                    session.sendResponse(put(id, request.getBody()));
                    return;
                case Request.METHOD_DELETE:
                    session.sendResponse(delete(id));
                    return;
                default:
                    session.sendResponse(new Response(Response.METHOD_NOT_ALLOWED));
            }
        } catch (NoSuchElementException e) {
            session.sendError(Response.NOT_FOUND, null);
        } catch (IOException e){
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
        session.sendError(Response.BAD_REQUEST, null);
    }

    private Response get(String id) throws IOException, NoSuchElementException{
        return new Response(Response.OK, dao.get(id.getBytes()));
    }

    private Response put(String id, byte[] value) throws IOException {
        dao.upsert(id.getBytes(), value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    private Response delete(String id) throws IOException {
        dao.remove(id.getBytes());
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

}
