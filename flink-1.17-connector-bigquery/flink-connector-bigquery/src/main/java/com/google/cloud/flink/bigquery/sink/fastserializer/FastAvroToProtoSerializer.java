package com.google.cloud.flink.bigquery.sink.fastserializer;

import com.google.api.client.util.Preconditions;
import com.google.cloud.flink.bigquery.sink.exceptions.BigQuerySerializationException;
import com.google.cloud.flink.bigquery.sink.serializer.BigQueryProtoSerializer;
import com.google.cloud.flink.bigquery.sink.serializer.BigQuerySchemaProvider;
import com.google.protobuf.ByteString;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.SchemaNormalization;
import org.apache.avro.generic.GenericRecord;

public class FastAvroToProtoSerializer extends BigQueryProtoSerializer<GenericRecord> {
    private Schema tableSchema;
    private SerializerRegistry serializerRegistery;

    /**
     * Prepares the serializer before its serialize method can be called. It allows contextual
     * preprocessing after constructor and before serialize. The Sink will internally call this
     * method when initializing itself.
     *
     * @param bigQuerySchemaProvider {@link BigQuerySchemaProvider} for the destination table.
     */
    @Override
    public void init(BigQuerySchemaProvider bigQuerySchemaProvider) {
        Preconditions.checkNotNull(
                bigQuerySchemaProvider,
                "BigQuerySchemaProvider not found while initializing FastAvroToProtoSerializer");
        Schema derivedDescriptor = bigQuerySchemaProvider.getAvroSchema();
        Preconditions.checkNotNull(
                derivedDescriptor, "Destination BigQuery table's Avro Schema could not be found.");
        this.tableSchema = derivedDescriptor;
        this.serializerRegistery = SerializerRegistry.getDefaultInstance();
    }

    @Override
    public ByteString serialize(GenericRecord record) throws BigQuerySerializationException {
        ProtoSerializer<GenericRecord> fastSerializer = serializerRegistery.getProtoSerializer(
                record.getSchema(),
                tableSchema);
        return fastSerializer.serialize(record);
    }
}
