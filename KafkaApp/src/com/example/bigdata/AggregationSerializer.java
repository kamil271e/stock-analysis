package com.example.bigdata;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;

public class AggregationSerializer extends StdSerializer<Aggregation> {
    public AggregationSerializer() {
        this(null);
    }

    public AggregationSerializer(Class<Aggregation> t) {
        super(t);
    }

    @Override
    public void serialize(Aggregation aggregation, JsonGenerator generator, SerializerProvider serializerProvider) throws IOException {
        generator.writeStartObject();
        generator.writeNumberField("sumClose", aggregation.getSumClose());
        generator.writeNumberField("minLow", aggregation.getMinLow());
        generator.writeNumberField("maxHigh", aggregation.getMaxHigh());
        generator.writeNumberField("sumVolume", aggregation.getSumVolume());
        generator.writeNumberField("count", aggregation.getCount());
        generator.writeEndObject();
    }
}

