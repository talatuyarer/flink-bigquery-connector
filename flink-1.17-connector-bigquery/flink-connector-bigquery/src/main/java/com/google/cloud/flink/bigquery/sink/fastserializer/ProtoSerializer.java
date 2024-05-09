package com.google.cloud.flink.bigquery.sink.fastserializer;

import com.google.protobuf.ByteString;

public interface ProtoSerializer<T> {
    ByteString serialize(T message);

}
