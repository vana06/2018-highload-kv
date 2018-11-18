package ru.mail.polis.vana06.handler;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.KVDao;
import ru.mail.polis.vana06.KVDaoServiceImpl;
import ru.mail.polis.vana06.RF;
import ru.mail.polis.vana06.Value;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

public class GetHandler extends RequestHandler {
    private List<Value> values = new ArrayList<>();

    public GetHandler(@NotNull String methodName, @NotNull KVDao dao, @NotNull RF rf, String id) {
        super(methodName, dao, rf, id, null);
    }

    @Override
    public Response onProxied() throws NoSuchElementException {
        Value value = dao.internalGet(id.getBytes());
        Response response = new Response(Response.OK, value.getData());
        response.addHeader(TIMESTAMP + String.valueOf(value.getTimestamp()));
        response.addHeader(STATE + value.getState().name());
        return response;
    }

    @Override
    public Callable<Boolean> ifMe() {
        return () -> {
            Value value = dao.internalGet(id.getBytes());
            values.add(value);
            return true;
        };
    }

    @Override
    public Callable<Boolean> ifNotMe(HttpClient client) {
        return () -> {
            final Response response = client.get(KVDaoServiceImpl.ENTITY_PATH + "?id=" + id, KVDaoServiceImpl.PROXY_HEADER);
            values.add(new Value(response.getBody(), Long.valueOf(response.getHeader(TIMESTAMP)),
                    Value.State.valueOf(response.getHeader(STATE))));
            return true;
        };
    }

    @Override
    public Response onSuccess(int acks) throws NoSuchElementException {
        Value max = values
                .stream()
                .max(Comparator.comparing(Value::getTimestamp))
                .get();
        if (max.getState() == Value.State.PRESENT) {
            return success(Response.OK, acks, max.getData());
        } else {
            throw new NoSuchElementException();
        }
    }

    @Override
    public Response onFail(int acks) {
        return gatewayTimeout(acks);
    }

    /**
     * @param futures список будущих ack
     * @return <ol>
     * <li> 200 OK и данные, если ответили хотя бы ack из from реплик </li>
     * <li> 404 Not Found, если ни одна из ack реплик, вернувших ответ, не содержит данные (либо данные удалены хотя бы на одной из ack ответивших реплик) </li>
     * <li> 504 Not Enough Replicas, если не получили 200/404 от ack реплик из всего множества from реплик </li>
     * </ol>
     */
    @Override
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        return super.getResponse(futures);
    }
}
