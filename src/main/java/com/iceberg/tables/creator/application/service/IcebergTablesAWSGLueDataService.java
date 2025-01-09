package com.iceberg.tables.creator.application.service;

import java.util.*;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

public interface IcebergTablesAWSGLueDataService {

    String IO_MANIFEST_CACHE_ENABLED_DEFAULT = "true";
    String IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT = "0";

    void setTableIdentifier(String namespace, String tableName);

    void loadTable() throws Exception;

    String getTableType() throws Exception;

    String getTableType(String database, String table) throws Exception;

    List<String> listTables(String namespace) throws Exception;

    boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception;

    boolean alterTable(String newSchema) throws Exception;

    boolean dropTable() throws Exception;

    List<List<String>> readTable() throws Exception, UnsupportedEncodingException;

    String getTableLocation() throws Exception;

    String getTableDataLocation() throws Exception;

    Snapshot getCurrentSnapshot() throws Exception;

    Long getCurrentSnapshotId() throws Exception;

    Iterable<Snapshot> getListOfSnapshots() throws Exception;

    String writeTable(String record, String outputFile) throws Exception;

    boolean commitTable(String dataFileName) throws Exception;

    boolean rewriteFiles(String dataFileName) throws Exception;

    Schema getTableSchema();

    List<Namespace> listNamespaces() throws Exception;

    boolean createNamespace(Namespace namespace) throws Exception, AlreadyExistsException, UnsupportedOperationException;

    boolean dropNamespace(Namespace namespace) throws Exception, NamespaceNotEmptyException;

    java.util.Map<java.lang.String, java.lang.String> loadNamespaceMetadata(Namespace namespace) throws Exception, NoSuchNamespaceException;

    boolean renameTable(TableIdentifier from, TableIdentifier to) throws Exception, NoSuchTableException, AlreadyExistsException;

    PartitionSpec getSpec() throws Exception;

    String getUUID() throws Exception;


}
