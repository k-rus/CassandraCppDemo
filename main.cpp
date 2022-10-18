#include <cassandra.h>

#include <iostream>
#include <fstream>

void ok(CassError error, const char *fmt...)
{
    va_list arg;
    if (error != CASS_OK)
    {
        fprintf(stderr, fmt, arg);
        throw error;
    }
}

void print_error(CassFuture *cass_future, const char *query)
{
    const char *message;
    size_t message_length;
    cass_future_error_message(cass_future, &message, &message_length);

    fprintf(stderr, "Unable to run: '%.*s'\n", (int)message_length, message);
    fprintf(stderr, "Query failed: %s\n", query);
}

void ok(CassFuture *cass_future, const char *query)
{
    CassError error = cass_future_error_code(cass_future);
    if (error != CASS_OK)
    {
        print_error(cass_future, query);
        cass_future_free(cass_future);
        throw error;
    }
}

CassFuture *execute_statement_get_future(CassSession *session, CassStatement *statement, const char *query)
{
    CassFuture *future = cass_session_execute(session, statement);
    CassError error = cass_future_error_code(future);
    cass_statement_free(statement);
    if (error != CASS_OK)
    {
        print_error(future, query);
        cass_future_free(future);
        throw error;
    }
    return future;
}

void execute_statement(CassSession *session, CassStatement *statement, const char *query)
{
    cass_future_free(execute_statement_get_future(session, statement, query));
}

CassFuture *execute_query_get_future(CassSession *session, const char *query)
{
    CassStatement *statement = cass_statement_new(query, 0);
    return execute_statement_get_future(session, statement, query);
}

void execute_query(CassSession *session, const char *query)
{
    cass_future_free(execute_query_get_future(session, query));
}

void create_keyspace(CassSession *session)
{
    printf("Creating keyspace sensor_data if not exists\n");
    const char *create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS sensor_data \
        WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : 1 }";
    execute_query(session, create_keyspace_query);
}

void connect_cluster(int argc, char **argv, CassCluster *cluster, CassSession *session)
{
    /* Add contact points */
    if (argc > 2)
    {
        ok(cass_cluster_set_cloud_secure_connection_bundle(cluster, argv[1]),
           "Unable to configure cloud using the secure connection bundle: %s\n",
           argv[1]);
        /* Set credentials provided when creating your database */
        cass_cluster_set_credentials(cluster, argv[2], argv[3]);
    }
    else
    {
        ok(cass_cluster_set_contact_points(cluster, "127.0.0.1"),
           "Actually this call cannot fail.\n");
    }

    /* Provide the cluster object as configuration to connect the session */
    ok(cass_session_connect(session, cluster), "Unable to connect: '%.*s'\n");

    if (argc <= 2)
        create_keyspace(session);
}

void read_version(CassSession *session)
{
    /* Build statement and execute query */
    const char *query = "SELECT release_version FROM system.local";

    CassFuture *result_future = execute_query_get_future(session, query);

    /* Retrieve result set and get the first row */
    const CassResult *result = cass_future_get_result(result_future);
    const CassRow *row = cass_result_first_row(result);

    if (row)
    {
        const CassValue *value = cass_row_get_column_by_name(row, "release_version");

        const char *release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length, release_version);
    }

    cass_result_free(result);
    cass_future_free(result_future);
}

void connect_keyspace(CassSession *session)
{
    printf("Changing to the keyspace sensor_data\n");
    std::string use_keyspace_query = "USE sensor_data";
    CassStatement *use_stmt = cass_statement_new(use_keyspace_query.c_str(), 0);
    ok(cass_session_execute(session, use_stmt), use_keyspace_query.c_str());
}

void recreate_table(CassSession *session)
{
    printf("Creating table sensors_by_network if needed \n");
    const char *create_table_query = "CREATE TABLE IF NOT EXISTS sensors_by_network ( \
    	network     text, \
	    sensor      text, \
    	temperature int, \
	    PRIMARY KEY ((network), sensor));";
    execute_query(session, create_table_query);
}

void simple_insert(CassSession *session)
{
    const char *insert_query = "INSERT INTO sensors_by_network(network, sensor, temperature) \
        VALUES ('volcano', 'v001', 210)";
    execute_query(session, insert_query);
}

void prepared_insert(CassSession *session)
{
    const char *insert_query = "INSERT INTO sensor_data.sensors_by_network \
        (network, sensor, temperature) \
        VALUES (?, ?, ?)";
    CassFuture *prepare_future = cass_session_prepare(session, insert_query);
    ok(prepare_future, insert_query);
    const CassPrepared *prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    CassStatement *insert_stmt = cass_prepared_bind(prepared);
    cass_statement_bind_string(insert_stmt, 0, "volcano");
    cass_statement_bind_string(insert_stmt, 1, "v002");
    cass_statement_bind_int32(insert_stmt, 2, 205);
    execute_statement(session, insert_stmt, insert_query);

    insert_stmt = cass_prepared_bind(prepared);
    cass_statement_bind_string(insert_stmt, 0, "forest");
    cass_statement_bind_string(insert_stmt, 1, "f001");
    cass_statement_bind_int32(insert_stmt, 2, 92);
    execute_statement(session, insert_stmt, insert_query);

    cass_prepared_free(prepared);
}

void read_by_key(CassSession *session, const char *key)
{
    const char *select_query = "SELECT * FROM sensors_by_network WHERE network = ?";
    CassStatement *select_stmt = cass_statement_new(select_query, 1);
    cass_statement_bind_string(select_stmt, 0, key);
    
    CassFuture *result_future = execute_statement_get_future(session, select_stmt, select_query);

    const CassResult *result = cass_future_get_result(result_future);
    CassIterator *iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(iterator))
    {
        const CassRow *row = cass_iterator_get_row(iterator);
        const CassValue *sensor_value = cass_row_get_column_by_name(row, "sensor");
        const CassValue *temp_value = cass_row_get_column_by_name(row, "temperature");

        printf("%s", key);

        const char *sensor;
        size_t sensor_length;
        cass_value_get_string(sensor_value, &sensor, &sensor_length);
        printf(", %.*s", (int)sensor_length, sensor);

        cass_int32_t temp;
        cass_value_get_int32(temp_value, &temp);
        printf(", %d\n", temp);
    }
    cass_iterator_free(iterator);
    cass_result_free(result);

    cass_future_free(result_future);
}

void read(CassSession *session)
{
    printf("============================\n");
    printf("network, sensor, temperature\n");
    printf("============================\n");
    read_by_key(session, "volcano");
    read_by_key(session, "forest");
    printf("\n");
}

int main(int argc, char **argv)
{
    CassError error_code = CASS_OK;
    CassCluster *cluster = cass_cluster_new();
    CassSession *session = cass_session_new();

    try
    {
        connect_cluster(argc, argv, cluster, session);

        read_version(session);
        connect_keyspace(session);
        recreate_table(session);
        simple_insert(session);
        prepared_insert(session);
        read(session);
    }
    catch (CassError error)
    {
        error_code = error;
    }
    /* Close the session */
    CassFuture *close_future = cass_session_close(session);
    cass_future_wait(close_future);
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);

    return error_code;
}
