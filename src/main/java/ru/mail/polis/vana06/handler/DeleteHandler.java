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

/**
 * Обработка DELETE запроса
 */
public class DeleteHandler extends RequestHandler {

    public DeleteHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id) {
        super(methodName, dao, rf, id, null);
    }

    @Override
    public Response onProxied() {
        dao.remove(id.getBytes());
        return new Response(Response.ACCEPTED, Response.EMPTY);
    }

    @Override
    public boolean ifMe() {
        dao.remove(id.getBytes());
        return true;
    }

    @Override
    public boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException {
        final Response response = client.delete(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id, KVDaoServiceImpl.PROXY_HEADER);
        return response.getStatus() == 202;
    }

    @Override
    public Response onSuccess(int acks) {
        return success(Response.ACCEPTED, acks, Response.EMPTY);
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    /**
     * @param acks набранное количество ack
     * @return <ol>
     * <li> 202 Accepted, если хотя бы ack из from реплик подтвердили операцию </li>
     * <li> 504 Not Enough Replicas, если не набралось ack подтверждений из всего множества from реплик </li>
     * </ol>
     */
    @Override
    public Response getResponse(int acks) {
        return super.getResponse(acks);
    }
}
