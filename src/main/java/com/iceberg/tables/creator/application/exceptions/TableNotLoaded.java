package com.iceberg.tables.creator.application.exceptions;

@SuppressWarnings("serial")
public class TableNotLoaded extends RuntimeException {
	
    public TableNotLoaded(String message) {
        super(message);
    }
    
}