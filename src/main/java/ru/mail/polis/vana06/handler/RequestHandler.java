package ru.mail.polis.vana06.handler;

import one.nio.http.HttpClient;
import one.nio.http.Response;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.mail.polis.KVDao;
import ru.mail.polis.vana06.KVDaoImpl;
import ru.mail.polis.vana06.RF;

import java.io.IOException;
import java.util.ArrayList;
import java.util.NoSuchElementException;
import java.util.concurrent.*;

/**
 * Шаблон для обработчика запросов
 */
public abstract class RequestHandler {
    private final String methodName;
    @NotNull
    final KVDaoImpl dao;
    @NotNull
    private final RF rf;
    final String id;
    final Long bytes;
    final byte[] value;

    final String TIMESTAMP = "timestamp";
    final String STATE = "state";

    private Logger log = LoggerFactory.getLogger(RequestHandler.class);

    /**
     * @param methodName название обрабатываемого метода
     * @param dao        хранилище данных
     * @param rf         replica factor
     * @param id         ключ
     * @param bytes       номер части
     * @param value      значение; может быть null и применяется только для обработки метода PUT
     */
    RequestHandler(String methodName, @NotNull KVDao dao, @NotNull RF rf, String id, Long bytes, byte[] value) {
        this.methodName = methodName;
        this.dao = (KVDaoImpl) dao;
        this.rf = rf;
        this.id = id;
        this.bytes = bytes;
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
     * @return Callable содержащий true, если не было исключений и все условия успешности запроса соблюдены
     * @throws IOException в случае внутрненних ошибок
     */
    public abstract Callable<Boolean> ifMe() throws IOException;

    /**
     * Обработка запроса если proxied == false и обработка проводится на другой ноде
     *
     * @param client нода, на которой должна проводится обработка
     * @return Callable содержащий true, если не было исключений и все условия успешности запроса соблюдены
     * <p>Callable может содержать следующий исключения <ol>
     * <li>InterruptedException   ошибка при запросе на клиент</li>
     * <li>PoolException          ошмбка при запросе на клиент</li>
     * <li>HttpException          ошибка при запросе на клиент</li>
     * <li>IOException            внутреннаяя ошибка на сервере</li>
     * <li>NoSuchElementException возвращает метод GET, если данные были удалены</li>
     * </ol>
     */
    public abstract Callable<Boolean> ifNotMe(HttpClient client);

    /**
     * Ответ на запрос при удачном выполнении, если ответили хотя бы ack из from реплик
     *
     * @param acks количество ответивших реплик
     * @return ответ на полученный запрос
     */
    abstract Response onSuccess(int acks);

    /**
     * Ответ на запрос при неудачном выполнении, если числов ответивших реплик ack меньше чем заданное число from
     *
     * @param acks количество ответивших реплик
     * @return ответ на полученный запрос
     */
    abstract Response onFail(int acks);

    /**
     * Проксирующая нода возвращает итоговый результат. <p>
     * В случае успеха вызывает {@code onSuccess(int acks)} <p>
     * В случае провала вызывает {@code onFail(int acks)}
     *
     * @param futures список будущих ack
     * @return ответ на полученный запрос
     */
    public Response getResponse(ArrayList<Future<Boolean>> futures) {
        int acks = 0;

        for (Future<Boolean> future : futures) {
            try {
                if (future.get()) {
                    acks++;
                    if (acks >= rf.getAck()) {
                        return onSuccess(acks);
                    }
                }
            } catch (ExecutionException | InterruptedException e) {
                log.error(e.getMessage(), e);
            }
        }

        return onFail(acks);
    }

    /**
     * Шаблон, формирующий ответ на запрос при его неудачном выполнении
     *
     * @param acks количество ответивших реплик
     * @return ответ на полученный запрос
     */
    Response gatewayTimeout(int acks) {
        log.info("Операция " + methodName + " не выполнена, acks = " + acks + " ; требования - " + rf);
        return new Response(Response.GATEWAY_TIMEOUT, Response.EMPTY);
    }

    /**
     * Шаблон, формирующий ответ на запрос при его удачном выполнении
     *
     * @param responseName имя запсроса, на который формируем ответ
     * @param acks         количество ответивших реплик
     * @param body         тело возвращаемого запроса
     * @return ответ на полученный запрос
     */
    Response success(String responseName, int acks, byte[] body) {
        log.info("Операция " + methodName + " выполнена успешно в " + acks + " нодах; требования - " + rf);
        return new Response(responseName, body);
    }
}
