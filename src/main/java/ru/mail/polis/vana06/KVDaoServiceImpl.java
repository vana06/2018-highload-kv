package ru.mail.polis.vana06;

import one.nio.http.*;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;
import one.nio.server.AcceptorConfig;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.KVService;

import java.io.IOException;
import java.io.NotSerializableException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Простой веб-сервер на основе one-nio
 *
 * @author Ivan Kylchik
 */
public class KVDaoServiceImpl extends HttpServer implements KVService {

    @NotNull
    private final KVDaoImpl dao;
    @NotNull
    private final String[] topology;

    private final RF defaultRF;
    private final Map<String, HttpClient> clients;

    private final String PROXY_HEADER = "proxied";
    private final String TIMESTAMP = "timestamp";

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
        this.dao = (KVDaoImpl) dao;
        this.topology = topology.toArray(new String[0]);
        defaultRF = new RF(this.topology.length / 2 + 1, this.topology.length);

        //clients = new IdentityHashMap<>(topology.size() - 1);
        clients = topology
                .stream()
                .filter(node -> !node.endsWith(String.valueOf(port)))
                .collect(Collectors.toMap(
                        o -> o,
                        o -> new HttpClient(new ConnectionString(o))));
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
    public void handler(Request request,
                        HttpSession session,
                        @Param("id=") String id,
                        @Param("replicas=") String replicas) throws IOException{

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
                return;
            }
        }

        String[] nodes;
        try {
            nodes = replicas(id, rf.getFrom());
        } catch (IllegalArgumentException e) {
            System.out.println(e.getMessage());
            session.sendError(Response.BAD_REQUEST, e.getMessage());
            return;
        }

        boolean proxied = request.getHeader(PROXY_HEADER) != null;

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

    private Response get(String id, String[] nodes, RF rf, boolean proxied) throws IOException, NoSuchElementException{
        //return new Response(Response.OK, dao.get(id.getBytes()));
        if(proxied){
            Value value = dao.internalGet(id.getBytes());
            if(value.getState() == Value.State.PRESENT){
                Response response = new Response(Response.OK, value.getData());
                response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
                return response;
            } else if(value.getState() == Value.State.REMOVED){
                Response response = new Response(Response.GONE, Response.EMPTY);
                response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
                return response;
            } else if (value.getState() == Value.State.ABSENT){
                throw new NoSuchElementException();
            }
        }

        List<Value> list = new ArrayList<>();
        String me = Arrays
                .stream(nodes)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElse(null);

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                Value value = dao.internalGet(id.getBytes());
                if(value.getState() == Value.State.PRESENT || value.getState() == Value.State.ABSENT){
                    acks++;
                    list.add(value);
                } else if(value.getState() == Value.State.REMOVED){
                    throw new NoSuchElementException();
                }
            } else {
                try {
                    final Response response = clients.get(node).get("/v0/entity?id=" + id, PROXY_HEADER);

                    if(response.getStatus() == 200){
                        list.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)), Value.State.PRESENT));
                        acks++;
                    } else if(response.getStatus() == 410) {
                        throw new NoSuchElementException();
                    } else if(response.getStatus() == 404){
                        list.add(new Value(new byte[]{}, -1, Value.State.ABSENT));
                        acks++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (PoolException e) {
                    e.printStackTrace();
                } catch (HttpException e) {
                    e.printStackTrace();
                }
            }
        }

        if(acks >= rf.getAck()){
            if(list.stream().anyMatch(value -> value.getState() == Value.State.PRESENT)){
                Value max = list.stream().max(Comparator.comparing(Value::getTimestamp)).get();
                return new Response(Response.OK, max.getData());
            } else {
                return new Response(Response.NOT_FOUND, Response.EMPTY);
            }
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response put(String id, byte[] value, String[] nodes, RF rf, boolean proxied) throws IOException {
        //dao.upsert(id.getBytes(), value);
        //return new Response(Response.CREATED, Response.EMPTY);
        if(proxied){
            dao.upsert(id.getBytes(), value);
            return new Response(Response.CREATED, Response.EMPTY);
        }

        String me = Arrays
                .stream(nodes)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElse(null);

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                dao.upsert(id.getBytes(), value);
                acks++;
            } else {
                try {
                    final Response response = clients.get(node).put("/v0/entity?id=" + id, value, PROXY_HEADER);
                    if(response.getStatus() == 201){
                        acks++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (PoolException e) {
                    e.printStackTrace();
                } catch (HttpException e) {
                    e.printStackTrace();
                }
            }
        }

        if(acks >= rf.getAck()){
            return new Response(Response.CREATED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }
    }

    private Response delete(String id, String[] nodes, RF rf, boolean proxied) throws IOException {
        //dao.remove(id.getBytes());
        //return new Response(Response.ACCEPTED, Response.EMPTY);
        if(proxied){
            dao.remove(id.getBytes());
            return new Response(Response.ACCEPTED, Response.EMPTY);
        }

        String me = Arrays
                .stream(nodes)
                .filter(node -> node.endsWith(String.valueOf(port)))
                .findFirst()
                .orElse(null);

        int acks = 0;
        for (final String node : nodes){
            if(node.equals(me)){
                dao.remove(id.getBytes());
                acks++;
            } else {
                try {
                    final Response response = clients.get(node).delete("/v0/entity?id=" + id, PROXY_HEADER);
                    if(response.getStatus() == 202){
                        acks++;
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (PoolException e) {
                    e.printStackTrace();
                } catch (HttpException e) {
                    e.printStackTrace();
                }
            }
        }

        if(acks >= rf.getAck()){
            return new Response(Response.ACCEPTED, Response.EMPTY);
        } else {
            return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
        }

    }

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

}
