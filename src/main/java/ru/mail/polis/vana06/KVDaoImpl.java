package ru.mail.polis.vana06;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
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
    private final HTreeMap<byte[], byte[]> storage;

    /**
     * Инициализирует хранилище
     * @param data - локальная папка для хранения данных
     */
    public KVDaoImpl(File data){
        File dataBase = new File(data, "dataBase");
        this.db = DBMaker
                .fileDB(dataBase)
                .fileMmapEnableIfSupported()
                .fileMmapPreclearDisable()
                .fileChannelEnable()
                .closeOnJvmShutdown()
                .make();
        this.storage = db.hashMap(data.getName())
                .keySerializer(Serializer.BYTE_ARRAY)
                .valueSerializer(Serializer.BYTE_ARRAY)
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
    public byte[] get(@NotNull byte[] key) throws NoSuchElementException {
        byte[] bytes = storage.get(key);
        if (bytes == null) {
            throw new NoSuchElementException();
        }
        return bytes;
    }

    /**
     * Метод добавляет в хранилище значение {@code value} если ключ {@code key}
     * не найден, иначе обновляет значение.
     *
     * @param key ключ для поиска
     * @param value значение для вставки/обновления
     */
    @Override
    public void upsert(@NotNull byte[] key, @NotNull byte[] value) {
        storage.put(key, value);
    }

    /**
     * Удаляет из хранилища элемент с ключом {@code key}
     *
     * @param key ключ для поиска
     */
    @Override
    public void remove(@NotNull byte[] key) {
        storage.remove(key);
    }

    /**
     * Закрыввает хранилище
     */
    @Override
    public void close() {
        db.close();
    }
}
