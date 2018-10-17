package ru.mail.polis.vana06;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DB;
import org.mapdb.DBMaker;
import org.mapdb.HTreeMap;
import org.mapdb.Serializer;
import org.mapdb.serializer.SerializerArrayTuple;
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
    private final HTreeMap<byte[], Object[]> storage;

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
                .valueSerializer(new SerializerArrayTuple(Serializer.BYTE_ARRAY, Serializer.LONG))
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
        Object[] bytes = storage.get(key);
        if (bytes == null) {
            throw new NoSuchElementException();
        }
        if((long)bytes[1] == -1){
            throw new NoSuchElementException("Element deleted");
        }
        return (byte[]) bytes[0];
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
        Object[] obj = new Object[]{value, System.nanoTime()};
        storage.put(key, obj);
    }

    /**
     * Удаляет из хранилища элемент с ключом {@code key}
     *
     * @param key ключ для поиска
     */
    @Override
    public void remove(@NotNull byte[] key) {
        //storage.remove(key);
        storage.put(key, new Object[]{new byte[]{} , -1L});
    }

    /**
     * Закрыввает хранилище
     */
    @Override
    public void close() {
        db.close();
    }
}
