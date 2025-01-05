package com.iceberg.tables.creator.application.catalog;

import java.util.ArrayList;
import java.util.List;

public class IcebergCatalogs {

    List<IcebergCustomGlueCatalog> catalogs;

    public IcebergCatalogs() {
        catalogs = new ArrayList<IcebergCustomGlueCatalog>();
    }

    public void addCatalog(IcebergCustomGlueCatalog catalog) {
        catalogs.add(catalog);
    }

    public List<IcebergCustomGlueCatalog> getCatalogs() {
        return catalogs;
    }

    public IcebergCustomGlueCatalog getCatalog(String catalogName, String metastoreUri) {
        if (catalogName == null || metastoreUri == null)
            return null;

        for (IcebergCustomGlueCatalog catalog : catalogs) {
            if (catalog.getName().equalsIgnoreCase(catalogName) && catalog.getMetastoreUri().equalsIgnoreCase(metastoreUri)) {
                return catalog;
            }
        }
        return null;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (IcebergCustomGlueCatalog catalog : catalogs) {
            sb.append(catalog);
            sb.append("\n");
        }

        return sb.toString();
    }
}
