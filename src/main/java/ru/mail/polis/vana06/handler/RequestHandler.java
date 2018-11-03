package ru.mail.polis.vana06.handler;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.http.Response;
import one.nio.pool.PoolException;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.vana06.KVDaoImpl;
import ru.mail.polis.vana06.RF;

import java.io.IOException;
import java.util.NoSuchElementException;

/**
 * Шаблон для обработчика запросов
 */
public abstract class RequestHandler {
    final String methodName;
    @NotNull
    final KVDaoImpl dao;
    @NotNull
    private final RF rf;
    final String id;
    final byte[] value;

    final String TIMESTAMP = "timestamp";
    final String STATE = "state";

    private Logger log = LoggerFactory.getLogger(RequestHandler.class);

    /**
     * @param methodName название обрабатываемого метода
     * @param dao        хранилище данных
     * @param rf         replica factor
     * @param id         ключ
     * @param value      значение; может быть null и применяется только для обработки метода PUT
     */
    RequestHandler(String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, byte[] value) {
        this.methodName = methodName;
        this.dao = (KVDaoImpl) dao;
        this.rf = rf;
        this.id = id;
        this.value = value;
    }

    /**
     * Обработка запроса если proxied == true
     *
     * @return ответ на запрос
     * @throws NoSuchElementException если метод GET не смог найти данные по ключу {@code id}
     */
    public abstract Response onProxied() throws NoSuchElementException;

    /**
     * Обработка запроса если proxied == false, но обработка выполняется для проксируемой ноды
     *
     * @return true если не было исключений и все условия успешности запроса соблюдены
     * @throws IOException в случае внутрненних ошибок
     */
    public abstract boolean ifMe() throws IOException;

    /**
     * Обработка запроса если proxied == false и обработка проводится на другой ноде
     *
     * @param client нода, на которой должна проводится обработка
     * @return true если не было исключений и все условия успешности запроса соблюдены
     * @throws InterruptedException   ошибка при запросе на клиент
     * @throws PoolException          ошмбка при запросе на клиент
     * @throws HttpException          ошибка при запросе на клиент
     * @throws IOException            внутреннаяя ошибка на сервере
     * @throws NoSuchElementException возвращает метод GET, если данные были удалены
     */
    public abstract boolean ifNotMe(HttpClient client) throws InterruptedException, PoolException, HttpException, IOException, NoSuchElementException;

    abstract Response onSuccess(int acks);

    abstract Response onFail(int acks);

    /**
     * Проксирующая нода возвращает итоговый результат. <p>
     * В случае успеха вызывает {@code onSuccess(int acks)} <p>
     * В случае провала вызывает {@code onFail(int acks)}
     *
     * @param acks набранное количество ack
     * @return ответ на полученный запрос
     */
    public Response getResponse(int acks) {
        if (acks >= rf.getAck()) {
            return onSuccess(acks);
        } else {
            return onFail(acks);
        }
    }

    Response gatewayTimeout(int acks) {
        log.info("Операция " + methodName + " не выполнена, acks = " + acks + " ; требования - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    Response success(String responseName, int acks, byte[] body) {
        log.info("Операция " + methodName + " выполнена успешно в " + acks + " нодах; требования - " + rf);
        return new Response(responseName, body);
    }
}
