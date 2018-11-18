package ru.mail.polis.vana06.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.vana06.KVDaoServiceImpl;
import ru.mail.polis.vana06.RF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

public class PutHandler extends RequestHandler {
    public PutHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, byte[] value) {
        super(methodName, dao, rf, id, value);
    }

    @Override
    public Response onProxied() {
        dao.upsert(id.getBytes(), value);
        return new Response(Response.CREATED, Response.EMPTY);
    }

    @Override
    public Callable<Boolean> ifMe() {
        return () -> {
            dao.upsert(id.getBytes(), value);
            return true;
        };
    }

    @Override
    public Callable<Boolean> ifNotMe(HttpClient client) {
        return () -> {
            final Response response = client.put(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id, value, KVDaoServiceImpl.PROXY_HEADER);
            return response.getStatus() == 201;
        };
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.CREATED, acks, Response.EMPTY);
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    /**
     * @param futures список будущих ack
     * @return <ol>
     * <li> 201 Created, если хотя бы ack из from реплик подтвердили операцию </li>
     * <li> 504 Not Enough Replicas, если не набралось ack подтверждений из всего множества from реплик </li>
     * </ol>
     */
    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}
