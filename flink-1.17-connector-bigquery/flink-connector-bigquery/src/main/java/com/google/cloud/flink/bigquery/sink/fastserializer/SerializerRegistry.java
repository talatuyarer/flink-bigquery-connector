package com.google.cloud.flink.bigquery.sink.fastserializer;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericRecord;

public class SerializerRegistry {
    private static volatile SerializerRegistry _INSTANCE;
    private static final Map<Schema, Long> SCHEMA_IDS_CACHE = new HashMap<>();

    private final ConcurrentHashMap<String, ProtoSerializer<GenericRecord>> protoSerializers = new ConcurrentHashMap<>();

    /**
     * Based on writerSchema and readerSchema returns Row Deserializer from the registry. If it can
     * not find any {@link ProtoSerializer} starts code generation.
     *
     * @param writerSchema the Flink record's avro schema
     * @param readerSchema the Bigquery Table's avro schema
     * @return
     */
    public ProtoSerializer<GenericRecord> getProtoSerializer(Schema writerSchema, Schema readerSchema) {
        String schemaKey = getSchemaKey(writerSchema, readerSchema);
        ProtoSerializer<GenericRecord> serializer = protoSerializers.get(schemaKey);

        //No serializer on the registry
        if (serializer == null) {
            //Lets build a serializer for this schema combination.
            Schema aliasedWriterSchema = Schema.applyAliases(writerSchema, readerSchema);
            serializer = ProtoSerializerGenerator.generateSerializer(GenericRecord.class, aliasedWriterSchema, readerSchema);
            protoSerializers.put(schemaKey, serializer);
        }
        return serializer;
    }


    /**
     * Get a new {@link SerializerRegistry} object.
     *
     * @return {@link SerializerRegistry}
     */
    public static SerializerRegistry getDefaultInstance() {
        if (_INSTANCE == null) {
            synchronized (SerializerRegistry.class) {
                if (_INSTANCE == null) {
                    _INSTANCE = new SerializerRegistry();
                }
            }
        }
        return _INSTANCE;
    }

    /**
     * This function will produce a fingerprint for the provided schema.
     *
     * @param schema a schema
     * @return fingerprint for the given schema
     */
    public static Long getSchemaFingerprint(Schema schema) {
        Long schemaId = SCHEMA_IDS_CACHE.get(schema);
        if (schemaId == null) {
            schemaId = SchemaNormalization.parsingFingerprint64(schema);
            SCHEMA_IDS_CACHE.put(schema, schemaId);
        }

        return schemaId;
    }

    public static String getSchemaKey(Schema writerSchema, Schema readerSchema) {
        return String.valueOf(Math.abs(getSchemaFingerprint(writerSchema))) + Math.abs(
                getSchemaFingerprint(readerSchema));
    }
}
