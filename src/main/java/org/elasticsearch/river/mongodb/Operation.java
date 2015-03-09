package org.elasticsearch.river.mongodb;

public enum Operation {
    INSERT(MongoDBRiver.OPLOG_INSERT_OPERATION),
    UPDATE(MongoDBRiver.OPLOG_UPDATE_OPERATION),
    UPDATE_ROW(MongoDBRiver.OPLOG_UPDATE_ROW_OPERATION),
    DELETE(MongoDBRiver.OPLOG_DELETE_OPERATION),
    DROP_COLLECTION("dc"),
    DROP_DATABASE("dd"),
    COMMAND(MongoDBRiver.OPLOG_COMMAND_OPERATION),
    UPDATE_TIMESTAMP("uptime"),  // Not a Mongo op, but we use it to propagate Timestamp updates from the slurper.
    UNKNOWN(null);

    private String value;

    private Operation(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    public static Operation fromString(String value) {
        if (value != null) {
            for (Operation operation : Operation.values()) {
                if (value.equalsIgnoreCase(operation.getValue())) {
                    return operation;
                }
            }
        }
        return Operation.UNKNOWN;
    }
}
