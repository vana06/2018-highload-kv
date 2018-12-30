package ru.mail.polis.vana06;

import org.jetbrains.annotations.NotNull;
import org.mapdb.DataInput2;
import org.mapdb.DataOutput2;
import org.mapdb.Serializer;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;

public class Key implements Serializable {
    private final ByteBuffer id;
    private final long part;

    Key(byte[] id, long part) {
        this.id = ByteBuffer.wrap(id);
        this.part = part;
    }

    public ByteBuffer getId() {
        return id;
    }

    Long getPart() {
        return part;
    }

    @Override
    public int hashCode() {
        //todo better hash
        return (int) (id.hashCode() + part);
    }

    @Override
    public boolean equals(Object obj) {
        if (getClass() != obj.getClass())
            return false;
        Key key = (Key) obj;
        return Arrays.equals(id.array(), key.id.array()) && part == key.part;
    }
}

class KeyCustomSerializer implements Serializer<Key>, Serializable {

    @Override
    public void serialize(@NotNull DataOutput2 out, @NotNull Key key) throws IOException {
        out.writeInt(key.getId().array().length);
        out.write(key.getId().array());
        out.writeLong(key.getPart());
    }

    @Override
    public Key deserialize(@NotNull DataInput2 input, int available) throws IOException {
        int length = input.readInt();
        byte[] id = new byte[length];
        input.readFully(id);

        long part = input.readLong();

        return new Key(id, part);
    }

}