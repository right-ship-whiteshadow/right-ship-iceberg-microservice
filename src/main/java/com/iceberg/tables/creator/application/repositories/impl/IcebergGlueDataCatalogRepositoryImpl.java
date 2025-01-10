package com.iceberg.tables.creator.application.repositories.impl;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.iceberg.*;
import org.apache.iceberg.metrics.ScanMetrics;
import org.springframework.beans.factory.annotation.Autowired;
import software.amazon.awssdk.http.SdkHttpClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.aws.glue.GlueCatalog;
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
import org.apache.iceberg.io.InputFile;
import org.apache.iceberg.io.LocationProvider;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.SerializableSupplier;
import org.apache.logging.log4j.LogManager;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonObject;
import com.iceberg.tables.creator.application.exceptions.TableNotFoundException;
import com.iceberg.tables.creator.application.exceptions.TableNotLoaded;
import com.iceberg.tables.creator.application.helpers.IcebergDataHelper;
import com.iceberg.tables.creator.application.repositories.IcebergGlueDataCatalogRepository;
import com.iceberg.tables.creator.application.security.AWSCredentials;
import com.iceberg.tables.creator.application.service.IcebergTablesAWSGLueDataService;

import jakarta.annotation.PostConstruct;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.glue.GlueClient;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

public class IcebergGlueDataCatalogRepositoryImpl implements IcebergGlueDataCatalogRepository {

    private GlueClient glueClient;

    private AWSCredentials awsCredentials;

    private TableIdentifier tableIdentifier;

    private JsonObject dataJsonObject;

    private Table icebergTable;

    private GlueCatalog glueCatalog;

    private Configuration configuration;

    private Long snapshotId;

    private String scanFilter;

    private TableScan scan;

    private static org.apache.logging.log4j.Logger log = LogManager
            .getLogger(IcebergGlueDataCatalogRepositoryImpl.class);

    public IcebergGlueDataCatalogRepositoryImpl(
            GlueCatalog glueCatalog, GlueClient glueClient, AWSCredentials awsCredentials) {
        this.awsCredentials = awsCredentials;
        this.glueCatalog = glueCatalog;
        this.glueClient = glueClient;
    }

    @PostConstruct
    public void setUpIcebergRepository() {
        configuration = new Configuration();
        configuration.set("fs.s3a.access.key", awsCredentials.getAwsClientAccessKey());
        configuration.set("fs.s3a.secret.key", awsCredentials.getAwsClientSecretKey());

        String endpoint = awsCredentials.getAwsEndPoint();
        if (endpoint != null) {
            configuration.set("fs.s3a.endpoint", endpoint);
            configuration.set("fs.s3a.path.style.access", "true");
        }

        if (configuration.get(IO_MANIFEST_CACHE_ENABLED) == null) {
            configuration.set(IO_MANIFEST_CACHE_ENABLED, IcebergTablesAWSGLueDataService.IO_MANIFEST_CACHE_ENABLED_DEFAULT);
        }
        if (configuration.get(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS) == null) {
            configuration.set(IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS,
                    IcebergTablesAWSGLueDataService.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS_DEFAULT);
        }
        Map<String, String> properties = new HashMap<>();
        properties.put("list-all-tables", "true");
        glueCatalog.setConf(configuration);
        glueCatalog.initialize(endpoint, properties);
    }

    public void setTableIdentifier(String namespace, String tableName) {
        tableIdentifier = TableIdentifier.of(namespace, tableName);
    }

    public Table loadTable(TableIdentifier identifier) {
        if (!glueCatalog.tableExists(identifier)) {
            throw new TableNotFoundException("ERROR: Table " + identifier + " does not exist");
        }

        Table table = glueCatalog.loadTable(identifier);
        if (table == null)
            throw new TableNotLoaded("ERROR Loading table: " + identifier);

        log.info(String.format("Table %s loaded successfully", table.name()));

        return table;
    }

    public void loadTable() {
        icebergTable = loadTable(tableIdentifier);

        scan = icebergTable.newScan();
        log.info(String.format("Scanning table %s using default snapshot", icebergTable.name()));
        if (snapshotId != null) {
            scan = scan.useSnapshot(snapshotId);
            log.info(String.format("Scanning table %s using provided snapshotId %d", icebergTable.name(), snapshotId));
        }
        if (scanFilter != null) {
            try {
                Expression filterExpr = ExpressionParser.fromJson(scanFilter);
                scan = scan.caseSensitive(false).ignoreResiduals().filter(filterExpr);
                log.info(String.format("Scanning table %s using provided filter", icebergTable.name(), scanFilter));
            } catch (Exception e) {
                log.error(String.format("Scanning table %s without a filter. Provided filter is invalid : %s",
                        icebergTable.name(), scanFilter));
            }
        }
    }

    public boolean createTable(Schema schema, PartitionSpec spec, boolean overwrite) {
        if (glueCatalog.tableExists(tableIdentifier)) {
            if (overwrite) {
                glueCatalog.dropTable(tableIdentifier);
            } else {
                throw new RuntimeException("Table " + tableIdentifier + " already exists");
            }
        }

        System.out.println("Creating table " + tableIdentifier);
        log.info(String.format("Creating table %s", tableIdentifier));

        Map<String, String> props = new HashMap<String, String>();
        glueCatalog.createTable(tableIdentifier, schema, spec, props);

        System.out.println("Table created successfully");
        log.info(String.format("Table %s created successfully", tableIdentifier));

        return true;
    }

    @Override
    public boolean alterTable(String newSchema) throws Exception {
        return false;
    }

    public boolean dropTable() {
        if (icebergTable == null)
            loadTable();

        System.out.println("Dropping the table " + tableIdentifier);
        log.info(String.format("Dropping table %s", tableIdentifier));
        if (glueCatalog.dropTable(tableIdentifier)) {
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
        Long snapshotId = getCurrentSnapshotId();
        if (snapshotId == null)
            return new ArrayList<List<String>>();
        IcebergGenerics.ScanBuilder scanBuilder = IcebergGenerics.read(icebergTable);
        if (scanFilter != null) {
            Expression filterExpr = ExpressionParser.fromJson(scanFilter);
            scanBuilder = scanBuilder.where(filterExpr).caseInsensitive();
        }

        CloseableIterable<Record> records = scanBuilder.useSnapshot(snapshotId).build();
        List<List<String>> output = new ArrayList<List<String>>();
        for (Record record : records) {
            int numFields = record.size();
            List<String> rec = new ArrayList<String>(numFields);
            for (int x = 0; x < numFields; x++) {
                Object value = record.get(x);
                rec.add(value == null ? "null" : value.toString());
            }
            output.add(rec);
        }
        return output;
    }

    @Override
    public Map<Integer, List<Map<String, String>>> getPlanFiles() throws IOException, URISyntaxException {
        if (icebergTable == null)
            loadTable();

        Iterable<FileScanTask> scanTasks = scan.planFiles();
        Map<Integer, List<Map<String, String>>> tasks = new HashMap<Integer, List<Map<String, String>>>();
        int index = 0;
        for (FileScanTask scanTask : scanTasks) {
            List<Map<String, String>> taskMapList = new ArrayList<Map<String, String>>();
            Map<String, String> taskMap = new HashMap<String, String>();
            DataFile file = scanTask.file();
            taskMap.put("content", file.content().toString());
            taskMap.put("file_path", file.path().toString());
            taskMap.put("file_format", file.format().toString());
            taskMap.put("start", Long.toString(scanTask.start()));
            taskMap.put("length", Long.toString(scanTask.length()));
            taskMap.put("spec", scanTask.spec().toString());
            taskMap.put("residual", scanTask.residual().toString());
            taskMapList.add(taskMap);

            tasks.put(index++, taskMapList);
        }

        logScanMetrics();

        return tasks;
    }

    @Override
    public Map<Integer, List<Map<String, String>>> getPlanTasks() throws IOException, URISyntaxException {
        if (icebergTable == null)
            loadTable();

        Iterable<CombinedScanTask> scanTasks = scan.planTasks();
        Map<Integer, List<Map<String, String>>> tasks = new HashMap<Integer, List<Map<String, String>>>();
        int index = 0;
        for (CombinedScanTask scanTask : scanTasks) {
            List<Map<String, String>> taskMapList = new ArrayList<Map<String, String>>();
            for (FileScanTask fileTask : scanTask.files()) {
                Map<String, String> taskMap = new HashMap<String, String>();
                DataFile file = fileTask.file();
                taskMap.put("content", file.content().toString());
                taskMap.put("file_path", file.path().toString());
                taskMap.put("file_format", file.format().toString());
                taskMap.put("start", Long.toString(fileTask.start()));
                taskMap.put("length", Long.toString(fileTask.length()));
                taskMap.put("spec", fileTask.spec().toString());
                taskMap.put("residual", fileTask.residual().toString());
                taskMapList.add(taskMap);
            }
            tasks.put(index++, taskMapList);
        }

        logScanMetrics();

        return tasks;
    }

    public void logScanMetrics() {
        try {
            Class<?> pifClass = Class.forName("org.apache.iceberg.BaseTableScan");
            java.lang.reflect.Field pf = pifClass.getDeclaredField("scanMetrics");
            pf.setAccessible(true);
            ScanMetrics metrics = (ScanMetrics) pf.get(scan);
            long numSkipped = metrics.skippedDataFiles().value();
            long numScanned = metrics.resultDataFiles().value();
            int pctSkipped = (int) (100 * (numSkipped / (double) (numScanned + numSkipped)));
            log.info("Skipped: " + numSkipped
                    + " Scanned: " + numScanned
                    + " Percent Skipped: " + pctSkipped);

        } catch (Exception e) {
            // Don't log the metrics
        }
    }

    public List<String> listTables(String namespace) {
        List<TableIdentifier> tables = glueCatalog.listTables(Namespace.of(namespace));
        return tables.stream().map(TableIdentifier::name).toList();
    }

    public java.util.List<Namespace> listNamespaces() {
        return glueCatalog.listNamespaces();
    }

    public boolean createNamespace(Namespace namespace) throws AlreadyExistsException, UnsupportedOperationException {
        glueCatalog.createNamespace(namespace);
        System.out.println("Namespace " + namespace + " created");
        return true;
    }

    public boolean dropNamespace(Namespace namespace) throws NamespaceNotEmptyException {
        if (glueCatalog.dropNamespace(namespace)) {
            System.out.println("Namespace " + namespace + " dropped");
            return true;
        }
        return false;
    }

    public boolean renameTable(TableIdentifier from, TableIdentifier to)
            throws NoSuchTableException, AlreadyExistsException {
        glueCatalog.renameTable(from, to);
        System.out.println("Table " + from + " renamed to " + to);

        return true;
    }

    public java.util.Map<java.lang.String, java.lang.String> loadNamespaceMetadata(Namespace namespace)
            throws NoSuchNamespaceException {
        return glueCatalog.loadNamespaceMetadata(namespace);
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
            // Verify if input columns are the same number as the required fields
            // Optional fields shouldn't be part of the check
            if (fieldNames.length > columns.size())
                throw new IllegalArgumentException(
                        "Number of fields in the record doesn't match the number of required columns in schema.\n");

            Record genericRecord = GenericRecord.create(schema);
            for (Types.NestedField col : columns) {
                String colName = col.name();
                Type colType = col.type();
                // Validate that a required field is present in the record
                if (!fields.has(colName)) {
                    if (col.isRequired())
                        throw new IllegalArgumentException("Record is missing a required field: " + colName);
                    else
                        continue;
                }

                // Trim the input value
                String value = fields.get(colName).toString().trim();

                // Check for null values
                if (col.isRequired() && value.equalsIgnoreCase("null"))
                    throw new IllegalArgumentException("Required field cannot be null: " + colName);

                // Store the value as an iceberg data type
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

            /*appender = Parquet.write(location).schema(schema).createWriterFunc(GenericParquetWriter::buildWriter)
                    .build();*/
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
                awsCredentials.getAwsClientAccessKey(),
                awsCredentials.getAwsClientSecretKey());

        SdkHttpClient client = ApacheHttpClient.builder()
                .maxConnections(100)
                .build();

        SerializableSupplier<S3Client> supplier = () -> {
            S3ClientBuilder clientBuilder = S3Client.builder()
                    .region(Region.of(awsCredentials.getAwsRegion()))
                    .credentialsProvider(StaticCredentialsProvider.create(awsCreds))
                    .httpClient(client);
            String uri = awsCredentials.getAwsEndPoint();
            if (uri != null) {
                clientBuilder.endpointOverride(URI.create(uri));
            }
            return clientBuilder.build();
        };

        return new S3FileIO(supplier);
    }

    public boolean commitTable(String dataFiles) throws Exception {
        if (icebergTable == null)
            loadTable();

        System.out.println("Commiting to the Iceberg table");

        S3FileIO io = null;
        try {
            io = initS3FileIO();

            JSONArray files = new JSONObject(dataFiles).getJSONArray("files");
            Transaction transaction = icebergTable.newTransaction();
            AppendFiles append = transaction.newAppend();
            // Commit data files
            System.out.println("Starting Txn");
            for (int index = 0; index < files.length(); ++index) {
                JSONObject file = files.getJSONObject(index);
                // Required
                String filePath = file.getString("file_path");

                // Optional (but slower if not given)
                String fileFormatStr = getJsonStringOrDefault(file, "file_format", null);
                Long fileSize = getJsonLongOrDefault(file, "file_size_in_bytes", null);
                Long numRecords = getJsonLongOrDefault(file, "record_count", null);

                append.appendFile(getDataFile(io, filePath, fileFormatStr, fileSize, numRecords));
            }
            append.commit();
            transaction.commitTransaction();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (io != null)
                io.close();
        }

        System.out.println("Txn Complete!");

        return true;
    }

    public boolean rewriteFiles(String dataFiles) throws Exception {
        if (icebergTable == null)
            loadTable();

        System.out.println("Rewriting files in the Iceberg table");

        S3FileIO io = null;

        Set<DataFile> oldDataFiles = new HashSet<DataFile>();
        Set<DataFile> newDataFiles = new HashSet<DataFile>();

        try {
            io = initS3FileIO();

            oldDataFiles = getDataFileSet(io, new JSONObject(dataFiles).getJSONArray("files_to_del"));
            newDataFiles = getDataFileSet(io, new JSONObject(dataFiles).getJSONArray("files_to_add"));

            Transaction transaction = icebergTable.newTransaction();
            RewriteFiles rewrite = transaction.newRewrite();

            // Rewrite data files
            System.out.println("Starting Txn");
            rewrite.rewriteFiles(oldDataFiles, newDataFiles);
            rewrite.commit();
            transaction.commitTransaction();
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (io != null)
                io.close();
        }

        System.out.println("Txn Complete!");

        return true;
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

    public DataFile getDataFile(S3FileIO io, String filePath, String fileFormatStr, Long fileSize, Long numRecords) throws Exception {
        PartitionSpec ps = icebergTable.spec();
        OutputFile outputFile = io.newOutputFile(filePath);

        if (fileFormatStr == null) {
            // if file format is not provided, we'll try to infer from the file extension (if any)
            String fileLocation = outputFile.location();
            if (fileLocation.contains("."))
                fileFormatStr = fileLocation.substring(fileLocation.lastIndexOf('.') + 1, fileLocation.length());
            else
                fileFormatStr = "";
        }

        FileFormat fileFormat = null;
        if (fileFormatStr.isEmpty())
            throw new Exception("Unable to infer the file format of the file to be committed: " + outputFile.location());
        else if (fileFormatStr.toLowerCase().equals("parquet"))
            fileFormat = FileFormat.PARQUET;
        else
            throw new Exception("Unsupported file format " + fileFormatStr + " cannot be committed: " + outputFile.location());

        if (fileSize == null) {
            try {
                FileSystem fs = FileSystem.get(new URI(outputFile.location()), configuration);
                FileStatus fstatus = fs.getFileStatus(new Path(outputFile.location()));
                fileSize = fstatus.getLen();
            } catch (Exception e) {
                throw new Exception("Unable to infer the filesize of the file to be committed: " + outputFile.location());
            }
        }

        if (numRecords == null) {
            try {
                Class<?> pifClass = Class.forName("org.apache.iceberg.parquet.ParquetIO");
                Constructor<?> pifCstr = pifClass.getDeclaredConstructor();
                pifCstr.setAccessible(true);
                Object pifInst = pifCstr.newInstance();
                Method pifMthd = pifClass.getDeclaredMethod("file", org.apache.iceberg.io.InputFile.class);
                pifMthd.setAccessible(true);
                org.apache.iceberg.io.InputFile pif = io.newInputFile(outputFile.location());
                Object parquetInputFile = pifMthd.invoke(pifInst, pif);

                /*ParquetFileReader reader = ParquetFileReader.open((InputFile) parquetInputFile);
                numRecords = reader.getRecordCount();*/
            } catch (Exception e) {
                throw new Exception("Unable to infer the number of records of the file to be committed: " + outputFile.location());
            }
        }

        DataFile data = DataFiles.builder(ps)
                .withPath(outputFile.location())
                .withFormat(fileFormat)
                .withFileSizeInBytes(fileSize)
                .withRecordCount(numRecords)
                .build();

        return data;
    }

    public Set<DataFile> getDataFileSet(S3FileIO io, JSONArray files) throws Exception {
        Set<DataFile> dataFiles = new HashSet<DataFile>();

        for (int index = 0; index < files.length(); ++index) {
            JSONObject file = files.getJSONObject(index);
            // Required
            String filePath = file.getString("file_path");

            // Optional (but slower if not given)
            String fileFormatStr = getJsonStringOrDefault(file, "file_format", null);
            Long fileSize = getJsonLongOrDefault(file, "file_size_in_bytes", null);
            Long numRecords = getJsonLongOrDefault(file, "record_count", null);

            try {
                dataFiles.add(getDataFile(
                        io,
                        filePath,
                        fileFormatStr,
                        fileSize,
                        numRecords));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return dataFiles;
    }
}
