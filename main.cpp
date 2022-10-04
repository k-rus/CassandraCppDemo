#include <cassandra.h>

#include <iostream>
#include <fstream>

const char *ks_name = "sensor_data";
const char *table_name = "sensors_by_network";

int replication_factor = 1;

const char *full_name = "sensor_data.sensors_by_network";

void check_and_throw(CassError error, const char *fmt...)
{
    va_list arg;
    if (error != CASS_OK)
    {
        fprintf(stderr, fmt, arg);
        throw error;
    }
}

void check_and_throw(CassFuture *cass_future, const char *cass_fmt)
{
    CassError error = cass_future_error_code(cass_future);
    if (error != CASS_OK)
    {
        /* Handle error */
        const char *message;
        size_t message_length;
        cass_future_error_message(cass_future, &message, &message_length);
        fprintf(stderr, "Unable to run: '%.*s'\n", (int)message_length, message);
        cass_future_free(cass_future);
        throw error;
    }
}

void create_keyspace(CassSession *session)
{
    std::string create_keyspace_query = "CREATE KEYSPACE IF NOT EXISTS ";
    create_keyspace_query.append(ks_name);
    create_keyspace_query.append(" WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'replication_factor' : ");
    create_keyspace_query.append(std::to_string(replication_factor));
    create_keyspace_query.append(" };");
    printf("Creating keyspace if not exists with query %s\n", create_keyspace_query.c_str());
    CassStatement *create_keyspace_statement = cass_statement_new(create_keyspace_query.c_str(), 0);
    check_and_throw(cass_session_execute(session, create_keyspace_statement), create_keyspace_query.c_str()); // TODO overload on query
}

void connect_cluster(int argc, char **argv, CassCluster *cluster, CassSession *session)
{
    /* Add contact points */
    if (argc > 2)
    {
        check_and_throw(cass_cluster_set_cloud_secure_connection_bundle(cluster, argv[1]),
                        "Unable to configure cloud using the secure connection bundle: %s\n",
                        argv[1]);
        /* Set credentials provided when creating your database */
        cass_cluster_set_credentials(cluster, argv[2], argv[3]);
        replication_factor = 3;
    }
    else
    {
        check_and_throw(cass_cluster_set_contact_points(cluster, "127.0.0.1"),
                        "Actually this call cannot fail.\n");
    }

    /* Provide the cluster object as configuration to connect the session */
    check_and_throw(cass_session_connect(session, cluster), "Unable to connect: '%.*s'\n");

    if (argc == 1)
        create_keyspace(session);
}

void read_version(CassSession *session);
void recreate_table(CassSession *session);
void connect_keyspace(CassSession *session);
void simple_insert(CassSession *session);
void parameterized_insert(CassSession *session);
void read(CassSession *session);

int main(int argc, char **argv)
{
    CassError error_code = CASS_OK;
    CassCluster *cluster = cass_cluster_new(); // I need it here to free it
    CassSession *session = cass_session_new(); // Session is used everywhere

    try
    {
        connect_cluster(argc, argv, cluster, session);

        read_version(session);
        connect_keyspace(session);
        recreate_table(session);
        simple_insert(session);
        parameterized_insert(session);
        read(session);
        std::cout << "All steps were executed.\n";
    }
    catch (CassError error)
    {
        error_code = error;
    }
    /* Close the session */
    CassFuture *close_future = cass_session_close(session);
    cass_future_wait(close_future); // TODO timeout
    cass_future_free(close_future);

    cass_cluster_free(cluster);
    cass_session_free(session);

    std::cout << "Bye, bye\n";

    return error_code;
}

void read_version(CassSession *session)
{

    /* Build statement and execute query */
    const char *query = "SELECT release_version FROM system.local";
    CassStatement *statement = cass_statement_new(query, 0);

    CassFuture *result_future = cass_session_execute(session, statement);
    check_and_throw(result_future,
                    "Unable to run query: '%.*s'\n");

    /* Retrieve result set and get the first row */
    const CassResult *result = cass_future_get_result(result_future);
    const CassRow *row = cass_result_first_row(result); // TODO free?

    if (row)
    {
        const CassValue *value = cass_row_get_column_by_name(row, "release_version"); // TODO free?

        const char *release_version;
        size_t release_version_length;
        cass_value_get_string(value, &release_version, &release_version_length);
        printf("release_version: '%.*s'\n", (int)release_version_length, release_version);
    }

    cass_result_free(result);

    cass_statement_free(statement);
    cass_future_free(result_future);
}

void connect_keyspace(CassSession *session)
{
    printf("Changing to the keyspace %s\n", ks_name);
    std::string use_keyspace_query = "USE ";
    use_keyspace_query.append(ks_name);
    CassStatement *use_stmt = cass_statement_new(use_keyspace_query.c_str(), 0);
    check_and_throw(cass_session_execute(session, use_stmt), use_keyspace_query.c_str());
}

void recreate_table(CassSession *session)
{
    std::string query = "SELECT table_name FROM system_schema.tables WHERE keyspace_name = '";
    query.append(ks_name);
    query.append("' and table_name = '");
    query.append(table_name);
    query.append("'");
    printf("Checking if table exists with query: %s\n", query.c_str());
    CassStatement *statement = cass_statement_new(query.c_str(), 0);

    CassFuture *result_future = cass_session_execute(session, statement);
    check_and_throw(result_future,
                    "Unable to run query: '%.*s'\n");

    const CassResult *result = cass_future_get_result(result_future);
    if (cass_result_row_count(result) > 0)
    {
        std::string drop_table_query = "DROP TABLE ";
        drop_table_query.append(full_name);
        printf("Dropping the table with query: %s\n", drop_table_query.c_str());
        CassStatement *drop_stmt = cass_statement_new(drop_table_query.c_str(), 0);
        check_and_throw(cass_session_execute(session, drop_stmt), drop_table_query.c_str());
    }
    cass_result_free(result);

    const char *create_table_query = "CREATE TABLE sensor_data.sensors_by_network ( \
    	network     text, \
	    sensor      text, \
    	temperature int, \
	    PRIMARY KEY ((network), sensor));";
    printf("Creating the table with: %s\n", create_table_query);
    const CassStatement *create_stmt = cass_statement_new(create_table_query, 0);
    check_and_throw(cass_session_execute(session, create_stmt), create_table_query);
}

void simple_insert(CassSession *session)
{
    const char *insert_query = "INSERT INTO sensors_by_network(network, sensor, temperature) \
        VALUES ('volcano', 'v001', 210)";
    CassStatement *insert_stmt = cass_statement_new(insert_query, 0);
    check_and_throw(cass_session_execute(session, insert_stmt), insert_query);
}

void parameterized_insert(CassSession *session)
{
    const char *insert_query = "INSERT INTO sensors_by_network(network, sensor, temperature) VALUES (?, ?, ?)";
    CassFuture *prepare_future = cass_session_prepare(session, insert_query);
    check_and_throw(prepare_future, "Prepared failed");
    const CassPrepared* prepared = cass_future_get_prepared(prepare_future);
    cass_future_free(prepare_future);

    CassStatement* insert_stmt = cass_prepared_bind(prepared);
    cass_statement_bind_string(insert_stmt, 0, "volcano");
    cass_statement_bind_string(insert_stmt, 1, "v002");
    cass_statement_bind_int32(insert_stmt, 2, 205);
    check_and_throw(cass_session_execute(session, insert_stmt), insert_query);

    insert_stmt = cass_prepared_bind(prepared);
    cass_statement_bind_string(insert_stmt, 0, "forest");
    cass_statement_bind_string(insert_stmt, 1, "f001");
    cass_statement_bind_int32(insert_stmt, 2, 92);
    check_and_throw(cass_session_execute(session, insert_stmt), insert_query);

    cass_prepared_free(prepared);
}

void read_by_key(CassSession *session, const char *key)
{
    const char *select_query = "SELECT * FROM sensors_by_network WHERE network = ?";
    CassStatement *select_stmt = cass_statement_new(select_query, 1);
    cass_statement_bind_string(select_stmt, 0, key);
    CassFuture *result_future = cass_session_execute(session, select_stmt);
    const CassResult *result = cass_future_get_result(result_future);
    CassIterator* iterator = cass_iterator_from_result(result);
    while (cass_iterator_next(iterator))
    {
        printf("%s", key);
        const CassRow* row = cass_iterator_get_row(iterator);
        const CassValue *sensor_value = cass_row_get_column_by_name(row, "sensor");
        const CassValue *temp_value = cass_row_get_column_by_name(row, "temperature");

        const char *sensor;
        size_t sensor_length;
        cass_value_get_string(sensor_value, &sensor, &sensor_length);
        printf(", '%.*s'", (int)sensor_length, sensor);

        cass_int32_t temp;
        cass_value_get_int32(temp_value, &temp);
        printf(", '%d'\n", temp);
    }
    cass_iterator_free(iterator);
    cass_result_free(result);

    cass_statement_free(select_stmt);
    cass_future_free(result_future);
}

void read(CassSession *session)
{
    printf("network, sensor, temperature\n");
    read_by_key(session, "volcano");
    read_by_key(session, "forest");
}
