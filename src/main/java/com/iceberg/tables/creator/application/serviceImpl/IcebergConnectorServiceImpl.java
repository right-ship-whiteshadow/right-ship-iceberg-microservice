package com.iceberg.tables.creator.application.serviceImpl;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.HasTableOperations;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.expressions.Expression;
import org.apache.iceberg.expressions.ExpressionParser;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileAppender;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.springframework.stereotype.Service;

import com.google.common.collect.ImmutableList;
import com.iceberg.tables.creator.application.catalog.IcebergCustomGlueCatalog;
import com.iceberg.tables.creator.application.exceptions.TableNotFoundException;
import com.iceberg.tables.creator.application.exceptions.TableNotLoaded;
import com.iceberg.tables.creator.application.helpers.IcebergDataHelper;
import com.iceberg.tables.creator.application.security.AWSCredentials;
import com.iceberg.tables.creator.application.service.IcebergConnectorService;

import jakarta.annotation.PostConstruct;

@Service
public class IcebergConnectorServiceImpl implements IcebergConnectorService {

    private Long snapshotId = null;
    private String scanFilter = null;
    private IcebergCustomGlueCatalog customGlueCatalog;
    private TableIdentifier tableIdentifier;
    private AWSCredentials credentials;
    private Table icebergTable;
    private TableScan scan;
    private final static String IO_MANIFEST_CACHE_ENABLED_DEFAULT = "true";
    private final static String IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT = "0";

    private static Logger log = LogManager.getLogger(IcebergConnectorServiceImpl.class);
    
    public IcebergConnectorServiceImpl(IcebergCustomGlueCatalog customGlueCatalog, String namespace, 
    		String tableName, AWSCredentials creds) throws IOException {
    	this.customGlueCatalog = customGlueCatalog;
    	this.credentials = creds;
        if (StringUtils.isNotBlank(tableName)) {
        	setTableIdentifier(namespace, tableName);
        }
    }
    
    @PostConstruct
    public void initialization() {
    	Configuration conf = new Configuration();
    	customGlueCatalog.setConf(conf);
    	if (credentials != null) {
            conf.set("fs.s3a.access.key", credentials.getAwsClientAccessKey());
            conf.set("fs.s3a.secret.key", credentials.getAwsClientSecretKey());
            
            String endpoint = credentials.getAwsEndPoint();
            if(endpoint != null) {
            	conf.set("fs.s3a.endpoint", endpoint);
            	conf.set("fs.s3a.path.style.access", "true");
            }
        }
    	if (conf.get(IO_MANIFEST_CACHE_ENABLED) == null) {
            conf.set(IO_MANIFEST_CACHE_ENABLED, IO_MANIFEST_CACHE_ENABLED_DEFAULT);
        }
        if (conf.get(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS) == null) {
            conf.set(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS, IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);
        }
        customGlueCatalog.getProperties().put("list-all-tables", "true");
                
        customGlueCatalog.initialize("Glue", customGlueCatalog.getProperties());
    }
    
    public void setTableIdentifier(String namespace, String tableName) {
        tableIdentifier = TableIdentifier.of(namespace, tableName);
    }
    
    public Table loadTable(TableIdentifier identifier) {
        if (!customGlueCatalog.tableExists(identifier)) {
            throw new TableNotFoundException("ERROR: Table " + identifier + " does not exist");
        }

        Table table = customGlueCatalog.loadTable(identifier);
        if (table == null)
            throw new TableNotLoaded("ERROR Loading table: " + identifier);
        
        log.info(String.format("Table %s loaded successfully", table.name()));
        
        return table;
    }
    
    public void loadTable() {
        icebergTable = loadTable(tableIdentifier);

        // Use snapshot passed by the user.
        // By default, use the latest snapshot.
        scan = icebergTable.newScan();
        log.info(String.format("Scanning table %s using default snapshot", icebergTable.name()));
        if (snapshotId != null) {
            scan = scan.useSnapshot(snapshotId);
            log.info(String.format("Scanning table %s using provided snapshotId %d", icebergTable.name(), snapshotId));
        }
        if (scanFilter != null) {
            try {
                Expression filterExpr = ExpressionParser.fromJson(scanFilter);
                scan = scan.caseSensitive(false)
                        .ignoreResiduals()
                        .filter(filterExpr);
                log.info(String.format("Scanning table %s using provided filter", icebergTable.name(), scanFilter));
            } catch (Exception e) {
                log.error(String.format("Scanning table %s without a filter. Provided filter is invalid : %s", icebergTable.name(), scanFilter));
            }
        }
    }
    
    public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) {
        if (customGlueCatalog.tableExists(tableIdentifier)) {
            if (overwrite) {
                // To overwrite an existing table, drop it first
            	customGlueCatalog.dropTable(tableIdentifier);
            } else {
                throw new RuntimeException("Table " + tableIdentifier + " already exists");
            }
        }
        
        System.out.println("Creating table " + tableIdentifier);
        log.info(String.format("Creating table %s", tableIdentifier));

        Map<String, String> props = new HashMap<String, String>();
        customGlueCatalog.createTable(tableIdentifier, schema, spec, props);
                
        System.out.println("Table created successfully");
        log.info(String.format("Table %s created successfully", tableIdentifier));
        
        return true;
    }
    
    public boolean dropTable() {
        if (icebergTable == null)
            loadTable();
        
        System.out.println("Dropping the table " + tableIdentifier);
        log.info(String.format("Dropping table %s", tableIdentifier));
        if (customGlueCatalog.dropTable(tableIdentifier)) {
            System.out.println("Table dropped successfully");
            log.info(String.format("Table %s dropped successfully", tableIdentifier));
            return true;
        }
        return false;
    }
    
    public List<List<String>> readTable() throws UnsupportedEncodingException {
        if (icebergTable == null)
            loadTable();
        
        System.out.println("Records in " + tableIdentifier + " :");
        // Use specified snapshot, latest by default
        Long snapshotId = getCurrentSnapshotId();
        if (snapshotId == null)
            return new ArrayList<List<String>>();
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(icebergTable);
        if(scanFilter != null) {
            Expression filterExpr = ExpressionParser.fromJson(scanFilter);
            scanBuilder = scanBuilder.where(filterExpr).caseInsensitive();
        }

        CloseableIterable<Record> records = scanBuilder.useSnapshot(snapshotId).build();
        List<List<String>> output = new ArrayList<List<String>>();
        for (Record record : records) {
            int numFields = record.size();
            List<String> rec = new ArrayList<String>(numFields);
            for(int x = 0; x < numFields; x++) {
                // A field can be optional, add a check for null values
                Object value = record.get(x);
                rec.add(value == null ? "null" : value.toString());
            }
            output.add(rec);
        }
        return output;
    }
    
    public List<String> listTables(String namespace) {
        List<TableIdentifier> tables = customGlueCatalog.listTables(Namespace.of(namespace));
        return tables.stream().map(TableIdentifier::name).toList();
    }
    
    public java.util.List<Namespace> listNamespaces() {
        return customGlueCatalog.listNamespaces();
    }
    
    public boolean createNamespace(Namespace namespace) throws AlreadyExistsException, UnsupportedOperationException {
    	customGlueCatalog.createNamespace(namespace);
        System.out.println("Namespace " + namespace + " created");
        return true;
    }
    
    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if(customGlueCatalog.dropNamespace(namespace)) {
            System.out.println("Namespace " + namespace + " dropped");
            return true;
        }
        return false;
    }
    
    public boolean renameTable(TableIdentifier from, TableIdentifier to) throws NoSuchTableException, AlreadyExistsException {
        customGlueCatalog.renameTable(from, to);
        System.out.println("Table " + from + " renamed to " + to);
        
        return true;
    }
    
    public java.util.Map<java.lang.String,java.lang.String> loadNamespaceMetadata(Namespace namespace) throws NoSuchNamespaceException {
        return customGlueCatalog.loadNamespaceMetadata(namespace);
    }
    
    public String getTableLocation() {
        if (icebergTable == null)
            loadTable();
        String tableLocation = icebergTable.location();
        if (tableLocation.endsWith("/"))
            return tableLocation.substring(0, tableLocation.length() - 1);
        return tableLocation;
    }

    public String getTableDataLocation() {
        if (icebergTable == null)
            loadTable();
        LocationProvider provider = icebergTable.locationProvider();
        String dataLocation = provider.newDataLocation("");
        if (dataLocation.endsWith("/"))
            return dataLocation.substring(0, dataLocation.length() - 1);
        return dataLocation;
    }
    
    public PartitionSpec getSpec() {
        if (icebergTable == null)
            loadTable();
        PartitionSpec spec = icebergTable.spec();
        return spec;
    }
    
    public String getUUID() {
        if (icebergTable == null)
            loadTable();
        TableMetadata metadata = ((HasTableOperations) icebergTable).operations().current();
        return metadata.uuid();
    }
         
    public Snapshot getCurrentSnapshot() {
        if (icebergTable == null)
            loadTable();
        return scan.snapshot();
    }
    
    public Long getCurrentSnapshotId() {
        if (icebergTable == null)
            loadTable();
        Snapshot snapshot = getCurrentSnapshot();
        if (snapshot != null)
            return snapshot.snapshotId();
        return null;
    }

    public java.lang.Iterable<Snapshot> getListOfSnapshots() {
        if (icebergTable == null)
            loadTable();
        Iterable<Snapshot> snapshots = icebergTable.snapshots();
        return snapshots;
    }
    
    public Schema getTableSchema() {
        if (icebergTable == null)
            loadTable();
        return scan.schema();
    }
    
    public String getTableType() throws Exception {
        if (icebergTable == null) {
            loadTable();
        }
        
        return "ICEBERG";
    }
    
    public String getTableType(String database, String table) throws Exception {
        loadTable(TableIdentifier.of(database, table));
        return "ICEBERG";
    }
    
    /*
    
    public String writeTable(String records, String outputFile) throws Exception {
        if (icebergTable == null)
            loadTable();
        
        System.out.println("Writing to the table " + tableIdentifier);
        
        // Check if outFilePath or name is passed by the user
        if (outputFile == null) {
            outputFile = String.format("%s/icebergdata-%s.parquet", getTableDataLocation(), UUID.randomUUID());
        }
        
        JSONObject result = new JSONObject();
        JSONArray files = new JSONArray();
        
        Schema schema = icebergTable.schema();
        ImmutableList.Builder<Record> builder = ImmutableList.builder();
        
        JSONArray listOfRecords = new JSONObject(records).getJSONArray("records");
        for (int index = 0; index < listOfRecords.length(); ++index) {
            JSONObject fields = listOfRecords.getJSONObject(index);
            List<Types.NestedField> columns = schema.columns();
            String[] fieldNames = JSONObject.getNames(fields);
            if (fieldNames.length > columns.size()) 
                throw new IllegalArgumentException("Number of fields in the record doesn't match the number of required columns in schema.\n");
            
            Record genericRecord = GenericRecord.create(schema);
            for (Types.NestedField col : columns) {
                String colName = col.name();
                Type colType = col.type();
                if (!fields.has(colName)) {
                    if (col.isRequired())
                        throw new IllegalArgumentException("Record is missing a required field: " + colName);
                    else
                        continue;
                }
                String value = fields.get(colName).toString().trim();
                if (col.isRequired() && value.equalsIgnoreCase("null"))
                    throw new IllegalArgumentException("Required field cannot be null: " + colName);
                genericRecord.setField(colName, IcebergDataHelper.stringToIcebergType(value, colType));
            }
            builder.add(genericRecord.copy());
        }
        
        S3FileIO io = null;
        FileAppender<Record> appender = null;
        try {
        io = initS3FileIO();
        OutputFile location = io.newOutputFile(outputFile);
        System.out.println("New file created at: " + location);
                    
        appender = Parquet.write(location)
                        .schema(schema)
                        .createWriterFunc(GenericParquetWriter::buildWriter)
                        .build();
        appender.addAll(builder.build());
        } finally {
            if (io != null)
                io.close();
            if (appender != null)
                appender.close();
        }
        
        // Add file info to the JSON object
        JSONObject file = new JSONObject();
        file.put("file_path", outputFile);
        file.put("file_format", FileFormat.fromFileName(outputFile));
        file.put("file_size_in_bytes", appender.length());
        file.put("record_count", listOfRecords.length());
        files.put(file);
        
        result.put("files", files);
                
        return result.toString();
    }
    
    public S3FileIO initS3FileIO() {
        AwsBasicCredentials awsCreds = AwsBasicCredentials.create(
                creds.getValue("AWS_ACCESS_KEY_ID"),
                creds.getValue("AWS_SECRET_ACCESS_KEY"));

        SdkHttpClient client = ApacheHttpClient.builder()
                .maxConnections(100)
                .build();
        
        SerializableSupplier<S3Client> supplier = () -> { 
            S3ClientBuilder clientBuilder = S3Client.builder()
                .region(Region.of(creds.getValue("AWS_REGION")))
                .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                .httpClient(client);
            String uri = creds.getValue("ENDPOINT");
            if (uri != null) {
                clientBuilder.endpointOverride(URI.create(uri));
            }
            return clientBuilder.build();
        };
        
        return  new S3FileIO(supplier);
    }
    */
    
    //TBD
    public String writeTable(String records, String outputFile) throws Exception {
    	return null;
    }

	// TBD
	@Override
	public boolean commitTable(String dataFileName) throws Exception {
		return false;
	}

	// TBD
	@Override
	public boolean rewriteFiles(String dataFileName) throws Exception {
		return false;
	}

	//TBD
	@Override
	public boolean alterTable(String newSchema) throws Exception {
		return false;
	}

    
}
