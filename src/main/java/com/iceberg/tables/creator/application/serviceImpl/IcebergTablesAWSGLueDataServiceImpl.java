package com.iceberg.tables.creator.application.serviceImpl;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.iceberg.*;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;

import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.io.OutputFile;
import org.apache.logging.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.iceberg.tables.creator.application.repositories.IcebergGlueDataCatalogRepository;
import com.iceberg.tables.creator.application.service.IcebergTablesAWSGLueDataService;
import jakarta.annotation.PostConstruct;


@Service
public class IcebergTablesAWSGLueDataServiceImpl implements IcebergTablesAWSGLueDataService {

	@Autowired
	private IcebergGlueDataCatalogRepository glueDataCatalogRepository;

	private static org.apache.logging.log4j.Logger log = LogManager
			.getLogger(IcebergTablesAWSGLueDataServiceImpl.class);

	@PostConstruct
	public void initialization() {

    }

	public void setTableIdentifier(String namespace, String tableName) {
		glueDataCatalogRepository.setTableIdentifier(namespace, tableName);
	}

	public Table loadTable(TableIdentifier identifier) {
		return glueDataCatalogRepository.loadTable(identifier);
	}

	public void loadTable() throws Exception {
		glueDataCatalogRepository.loadTable();
	}

	public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) throws Exception {
		return glueDataCatalogRepository.createTable(schema, spec, overwrite);
	}

    public boolean alterTable(String newSchema) throws Exception {
        return glueDataCatalogRepository.alterTable(newSchema);
    }


    public boolean dropTable() throws Exception {
		return glueDataCatalogRepository.dropTable();
	}

	public List<List<String>> readTable() throws Exception {
		return glueDataCatalogRepository.readTable();
	}

	public List<String> listTables(String namespace) throws Exception {
		return glueDataCatalogRepository.listTables(namespace);
	}

	public java.util.List<Namespace> listNamespaces() throws Exception {

		return glueDataCatalogRepository.listNamespaces();
	}

	public boolean createNamespace(Namespace namespace) throws Exception {
		return glueDataCatalogRepository.createNamespace(namespace);
	}

	public boolean dropNamespace(Namespace namespace) throws Exception {
		return glueDataCatalogRepository.dropNamespace(namespace);
	}

	public boolean renameTable(TableIdentifier from, TableIdentifier to)
            throws Exception {
		return glueDataCatalogRepository.renameTable(from, to);
	}

	public java.util.Map<java.lang.String, java.lang.String> loadNamespaceMetadata(Namespace namespace)
            throws Exception {
		return glueDataCatalogRepository.loadNamespaceMetadata(namespace);
	}

	public String getTableLocation() throws Exception {
		return glueDataCatalogRepository.getTableLocation();
	}

	public String getTableDataLocation() throws Exception {
		return glueDataCatalogRepository.getTableDataLocation();
	}

	public PartitionSpec getSpec() throws Exception {
		return glueDataCatalogRepository.getSpec();
	}

	public String getUUID() throws Exception {
		return glueDataCatalogRepository.getUUID();
	}

	public Snapshot getCurrentSnapshot() throws Exception {
		return glueDataCatalogRepository.getCurrentSnapshot();
	}

	public Long getCurrentSnapshotId() throws Exception {
		return glueDataCatalogRepository.getCurrentSnapshotId();
	}

	public java.lang.Iterable<Snapshot> getListOfSnapshots() throws Exception {
		return glueDataCatalogRepository.getListOfSnapshots();
	}

	public Schema getTableSchema() {
		return glueDataCatalogRepository.getTableSchema();
	}

	public String getTableType() throws Exception {
		return glueDataCatalogRepository.getTableType();
	}

	public String getTableType(String database, String table) throws Exception {
		return glueDataCatalogRepository.getTableType(database, table);
	}

	public String writeTable(String records, String outputFile) throws Exception {
		return glueDataCatalogRepository.getTableType(records, outputFile);
	}

	String getJsonStringOrDefault(JSONObject o, String key, String defVal) {
		try {
			return o.getString(key);
		} catch (JSONException e) {
			return defVal;
		}
	}

	Long getJsonLongOrDefault(JSONObject o, String key, Long defVal) {
		try {
			return o.getLong(key);
		} catch (JSONException e) {
			return defVal;
		}
	}

	DataFile getDataFile(S3FileIO io, String filePath, String fileFormatStr, Long fileSize, Long numRecords) throws Exception {
		return glueDataCatalogRepository.getDataFile(io, filePath, fileFormatStr, fileSize, numRecords);
	}

	Set<DataFile> getDataFileSet(S3FileIO io, JSONArray files) throws Exception {
		return glueDataCatalogRepository.getDataFileSet(io, files);
	}

	public boolean commitTable(String dataFiles) throws Exception {
		return glueDataCatalogRepository.commitTable(dataFiles);
	}

	public boolean rewriteFiles(String dataFiles) throws Exception {
		return glueDataCatalogRepository.rewriteFiles(dataFiles);
	}

}
