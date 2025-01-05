package com.iceberg.tables.creator.application.helpers;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.iceberg.DataFile;
import org.apache.iceberg.FileScanTask;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import org.apache.iceberg.types.Types;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.UUID;

public class IcebergDataHelper {

    public static Object stringToIcebergType(String value, Type colType) throws UnsupportedEncodingException {
        if (value.equalsIgnoreCase("null"))
            return null;

        switch (colType.typeId()) {
            case BOOLEAN:
                return value.equalsIgnoreCase("true");
            case INTEGER:
                return Integer.parseInt(value);
            case LONG:
                return Long.parseLong(value);
            case FLOAT:
                return Float.parseFloat(value);
            case DOUBLE:
                return Double.parseDouble(value);
            case STRING:
                return value;
            case UUID:
                UUID uuid = UUID.fromString(value);
                ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
                buffer.putLong(uuid.getMostSignificantBits());
                buffer.putLong(uuid.getLeastSignificantBits());
                return buffer.array();
            case FIXED:
                Types.FixedType fixed = (Types.FixedType) colType;
                return Arrays.copyOf(value.getBytes("UTF-8"), fixed.length());
            case BINARY:
                byte[] binaryValue = value.getBytes("UTF-8");
                return ByteBuffer.wrap(binaryValue);
            case DECIMAL:
                return new BigDecimal(value);
            case DATE:
                return LocalDate.parse(value);
            case TIME:
                return LocalTime.parse(value);
            case TIMESTAMP:
                if (colType == Types.TimestampType.withZone()) {
                    return OffsetDateTime.parse(value);
                } else {
                    return LocalDateTime.parse(value);
                }
            default:
                throw new IllegalArgumentException("Unsupported column type");
        }
    }

    public static String schemaAsCsv(Schema schema) {
        StringBuilder builder = new StringBuilder();

        List<Types.NestedField> columns = schema.columns();
        for (Types.NestedField col : columns) {
            Type colType = col.type();
            String type;
            int precision = 0;
            int scale = 0;
            switch (colType.typeId()) {
                case BOOLEAN:
                    type = "boolean";
                    break;
                case INTEGER:
                    type = "int";
                    break;
                case LONG:
                    type = "long";
                    break;
                case FLOAT:
                    type = "float";
                    break;
                case DOUBLE:
                    type = "double";
                    break;
                case STRING:
                    type = "string";
                    break;
                case UUID:
                    type = "uuid";
                    break;
                case FIXED:
                    type = "fixed";
                    Types.FixedType fixed = (Types.FixedType) colType;
                    precision = fixed.length();
                    break;
                case BINARY:
                    type = "binary";
                    break;
                case DECIMAL:
                    type = "decimal";
                    Types.DecimalType decimal = (Types.DecimalType) colType;
                    precision = decimal.precision();
                    scale = decimal.scale();
                    break;
                case DATE:
                    type = "date";
                    break;
                case TIME:
                    type = "time";
                    break;
                case TIMESTAMP:
                    if (colType == Types.TimestampType.withZone()) {
                        type = "timestamp";
                    } else {
                        type = "timestamptz";
                    }
                    break;
                default:
                    throw new IllegalArgumentException("Unsupported column type");
            }

            String columnInfo = String.format("%d,%s,%s,%d,%d,%b\n", col.fieldId(), col.name(), type, precision, scale, col.isRequired());
            builder.append(columnInfo);
        }

        return builder.toString();
    }
}
