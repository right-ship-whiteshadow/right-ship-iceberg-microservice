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
    //private List<IcebergTableAWSGlueDataEntity> icebergUniversalTableEntities;
    
    private IcebergTableAWSGlueDataEntity icebergTableAWSGlueDataEntity;
    
    private static Logger log = LogManager.getLogger(IcebergTableAWSGlueDataEntities.class);

    public IcebergTableAWSGlueDataEntities() {

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

	/*public List<IcebergTableAWSGlueDataEntity> getIcebergUniversalTableEntities() {
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
	}*/

}
