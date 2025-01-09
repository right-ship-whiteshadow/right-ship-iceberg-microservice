package com.iceberg.tables.creator.application.catalog;

import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hadoop.conf.Configuration;
import com.fasterxml.jackson.core.JacksonException;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;

public class IcebergCustomCatalogDeserializer extends StdDeserializer<IcebergGlueCatalog> {

    public IcebergCustomCatalogDeserializer() {
        this(null);
    }

    public IcebergCustomCatalogDeserializer(Class<?> vc) {
        super(vc);
    }

    @Override
    public IcebergGlueCatalog deserialize(JsonParser p, DeserializationContext context) throws IOException, JacksonException {
        JsonNode node = p.getCodec().readTree(p);
        String name = node.get("name").asText();
        IcebergCatalogType type = IcebergCatalogType.valueOf(node.get("type").asText());
        String metastoreUri = node.get("metastoreUri").asText();
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> properties = mapper.convertValue(node.get("properties"), new TypeReference<Map<String, String>>(){});
        Map<String, String> conf = mapper.convertValue(node.get("conf"), new TypeReference<Map<String, String>>(){});
        Configuration catalogConf = new Configuration();
        for (Entry<String, String> entry : conf.entrySet()) {
            catalogConf.set(entry.getKey(), entry.getValue());
        }

        return new IcebergGlueCatalog(name, properties, catalogConf);
    }

}
