package com.iceberg.tables.creator.application.catalog;

public enum IcebergCatalogType {
	
	HADOOP("org.apache.iceberg.hadoop.HadoopCatalog"),
    GLUE("org.apache.iceberg.aws.glue.GlueCatalog");

    public final String type;

    private IcebergCatalogType(String type) {
        this.type = type;
    }

}
