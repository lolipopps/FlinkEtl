package com.flink.schema;

import com.flink.bean.OrderSchemaEvent;
import com.google.gson.Gson;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Preconditions;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;


@PublicEvolving
public class OrderEventSchema implements DeserializationSchema<OrderSchemaEvent>, SerializationSchema<OrderSchemaEvent> {
    private static final long serialVersionUID = 1L;
    private transient Charset charset;

    public OrderEventSchema() {
        this(StandardCharsets.UTF_8);
    }

    public OrderEventSchema(Charset charset) {
        this.charset = (Charset) Preconditions.checkNotNull(charset);
    }

    public Charset getCharset() {
        return this.charset;
    }


    @Override
    public OrderSchemaEvent deserialize(byte[] message) {
        Gson gson = new Gson();
        return gson.fromJson(new String(message, this.charset), OrderSchemaEvent.class);
    }


    @Override
    public boolean isEndOfStream(OrderSchemaEvent nextElement) {
        return false;
    }


    @Override
    public byte[] serialize(OrderSchemaEvent clickSchemaEvent) {
        return clickSchemaEvent.toString().getBytes();
    }


    @Override
    public TypeInformation<OrderSchemaEvent> getProducedType() {
        return TypeInformation.of(OrderSchemaEvent.class);
    }


    private void writeObject(ObjectOutputStream out) throws IOException {
        out.defaultWriteObject();
        out.writeUTF(this.charset.name());
    }


    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        String charsetName = in.readUTF();
        this.charset = Charset.forName(charsetName);
    }
}
