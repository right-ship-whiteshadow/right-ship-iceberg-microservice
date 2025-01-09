package com.iceberg.tables.creator.application.tables.datas;

import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_ENABLED;
import static org.apache.iceberg.CatalogProperties.IO_MANIFEST_CACHE_EXPIRATION_INTERVAL_MS;

import java.util.List;

import org.apache.iceberg.TableScan;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.iceberg.tables.creator.application.tables.models.IcebergTableAWSGlueDataEntity;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/*@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor*/
public class IcebergTableAWSGlueDataEntities {

    private Long snapshotId;
    private String scanFilter;
    private List<IcebergTableAWSGlueDataEntity> icebergUniversalTableEntities;
    private TableScan scan;

    private static Logger log = LogManager.getLogger(IcebergTableAWSGlueDataEntities.class);

    public IcebergTableAWSGlueDataEntities() {
    	if (credentials != null) {
            conf.set("fs.s3a.access.key", credentials.getAwsClientAccessKey());
            conf.set("fs.s3a.secret.key", credentials.getAwsClientSecretKey());

            String endpoint = credentials.getAwsEndPoint();
            if (endpoint != null) {
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
    }

	public Long getSnapshotId() {
		return snapshotId;
	}

	public void setSnapshotId(Long snapshotId) {
		this.snapshotId = snapshotId;
	}

	public String getScanFilter() {
		return scanFilter;
	}

	public void setScanFilter(String scanFilter) {
		this.scanFilter = scanFilter;
	}

	public List<IcebergTableAWSGlueDataEntity> getIcebergUniversalTableEntities() {
		return icebergUniversalTableEntities;
	}

	public void setIcebergUniversalTableEntities(List<IcebergTableAWSGlueDataEntity> icebergUniversalTableEntities) {
		this.icebergUniversalTableEntities = icebergUniversalTableEntities;
	}

	public TableScan getScan() {
		return scan;
	}

	public void setScan(TableScan scan) {
		this.scan = scan;
	}

}
