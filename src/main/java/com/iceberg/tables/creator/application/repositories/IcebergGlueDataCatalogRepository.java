package com.iceberg.tables.creator.application.repositories;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.json.JSONArray;

public interface IcebergGlueDataCatalogRepository {

    void setTableIdentifier(String namespace, String tableName);

    void loadTable() throws Exception;

    Table loadTable(TableIdentifier identifier);

    String getTableType() throws Exception;

    String getTableType(String database, String table) throws Exception;

    List<String> listTables(String namespace) throws Exception;

    boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception;

    boolean alterTable(String newSchema) throws Exception;

    boolean dropTable() throws Exception;

    List<List<String>> readTable() throws Exception, UnsupportedEncodingException;

    Map<Integer, List<Map<String, String>>> getPlanFiles() throws IOException, URISyntaxException;

    Map<Integer, List<Map<String, String>>> getPlanTasks() throws IOException, URISyntaxException;

    String getTableLocation() throws Exception;

    String getTableDataLocation() throws Exception;

    Snapshot getCurrentSnapshot() throws Exception;

    Long getCurrentSnapshotId() throws Exception;

    Iterable<Snapshot> getListOfSnapshots() throws Exception;

    String writeTable(String record, String outputFile) throws Exception, UnsupportedEncodingException;

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

    DataFile getDataFile(S3FileIO io, String filePath, String fileFormatStr, Long fileSize, Long numRecords) throws Exception;

    Set<DataFile> getDataFileSet(S3FileIO io, JSONArray files) throws Exception;

}