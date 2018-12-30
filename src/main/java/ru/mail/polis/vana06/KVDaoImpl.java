package ru.mail.polis.vana06;

import org.jetbrains.annotations.NotNull;
import org.mapdb.*;
import ru.mail.polis.KVDao;

import java.io.File;
import java.util.NoSuchElementException;

/**
 * Реализация Key-value DAO на основе DBMaker
 *
 * @author Ivan Kylchik
 */
public class KVDaoImpl implements KVDao {

    private final DB db;
    private final HTreeMap<Key, Value> storage;

    /**
     * Инициализирует хранилище
     *
     * @param data - локальная папка для хранения данных
     */
    public KVDaoImpl(File data) {
        File dataBase = new File(data, "dataBase");
        Serializer<Value> valueSerializer = new ValueCustomSerializer();
        Serializer<Key> keySerializer = new KeyCustomSerializer();
        this.db = DBMaker
                .fileDB(dataBase)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(keySerializer)
                .valueSerializer(valueSerializer)
                .createOrOpen();
    }

    /**
     * Метод возвращает значение из хранилища по ключю {@code key}
     *
     * @param key ключ для поиска
     * @return значение соответствующее ключу {@code key}
     * @throws NoSuchElementException если элемент не был найден
     */
    @NotNull
    @Override
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException, IllegalStateException {
        Value value = internalGet(key, 0);

        if (value.getState() == Value.State.ABSENT || value.getState() == Value.State.REMOVED) {
            throw new NoSuchElementException();
        }
        return value.getData();
    }

    /**
     * Расширенная версия метода get.
     *
     * @param key  ключ для поиска
     * @param part номер части файла
     * @return объект класса {@code Value}, помимо данных содержит ключевую информацию о хранимом объекте
     */
    @NotNull
    public Value internalGet(@NotNull byte[] key, long part) {
        Value value = storage.get(new Key(key, part));
        if (value == null) {
            return new Value(new byte[]{}, 0, Value.State.ABSENT);
        }
        return value;
    }

    /**
     * Метод добавляет в хранилище значение {@code value} если ключ {@code key}
     * не найден, иначе обновляет значение.
     *
     * @param key   ключ для поиска
     * @param value значение для вставки/обновления
     */
    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        upsert(key, 0, value);
    }

    public void upsert(@NotNull byte[] id, long part, @NotNull byte[] value) {
        storage.put(new Key(id, part), new Value(value, System.currentTimeMillis(), Value.State.PRESENT));
    }

    /**
     * Удаляет из хранилища элемент с ключом {@code key}
     *
     * @param key ключ для поиска
     */
    @Override
    public void remove(@NotNull byte[] key) {
        remove(key, 0);
    }

    /**
     * Удаляет из хранилища элемент с ключом {@code key} и частью номер {@code part}
     *
     * @param key  ключ для поиска
     * @param part номер части файла
     */
    public void remove(@NotNull byte[] key, long part) {
        storage.put(new Key(key, part), new Value(new byte[]{}, System.currentTimeMillis(), Value.State.REMOVED));
    }

    /**
     * Закрыввает хранилище
     */
    @Override
    public void close() {
        db.close();
    }
}
