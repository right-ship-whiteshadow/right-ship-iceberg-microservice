package com.iceberg.tables.creator.application.exceptions;

@SuppressWarnings("serial")
public class TableNotFoundException extends RuntimeException {
	
    public TableNotFoundException(String message) {
        super(message);
    }
    
}
