package com.experoinc.janusgraph.diskstorage.foundationdb;

import org.janusgraph.diskstorage.PermanentBackendException;

public class FoundationDBTxException extends PermanentBackendException {

    private static final long serialVersionUID = 1L;

    public static String TIMEOUT = "Transaction is too old to perform reads or be committed";
    public static String INTERRUPTED = "Interrupted while waiting for FoundationDB result";
    public static String CLOSED_WHILE_ACTIVE = "Active transaction closed by another thread";

    public FoundationDBTxException(String msg) {
        super(msg);
    }

    public FoundationDBTxException(Throwable cause) {
        super(cause);
    }

    public FoundationDBTxException(String msg, Throwable cause) {
        super(msg, cause);
    }
}