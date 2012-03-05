package edu.uci.ics.asterix.aql.base;

public interface Statement extends IAqlExpression {
    public enum Kind {
        DATASET_DECL,
        DATAVERSE_DECL,
        DATAVERSE_DROP,
        DATASET_DROP,
        DELETE,
        INSERT,
        UPDATE,
        DML_CMD_LIST,
        FUNCTION_DECL,
        LOAD_FROM_FILE,
        WRITE_FROM_QUERY_RESULT,
        NODEGROUP_DECL,
        NODEGROUP_DROP,
        QUERY,
        SET,
        TYPE_DECL,
        TYPE_DROP,
        WRITE,
        CREATE_INDEX,
        INDEX_DECL,
        CREATE_DATAVERSE,
        INDEX_DROP,
        BEGIN_FEED,
        CONTROL_FEED,
        CREATE_FUNCTION,
        FUNCTION_DROP
    }

    public abstract Kind getKind();

}
