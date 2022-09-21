#include <cassandra.h>

#include <iostream>

void check_and_throw(CassError error, const char *fmt...)
{
    va_list arg;
    if (error != CASS_OK)
    {
        fprintf(stderr, fmt, arg);
        throw error;
    }
}

void check_and_throw(CassFuture *connect_future, const char *cass_fmt)
{
    CassError error = cass_future_error_code(connect_future);
    if (error != CASS_OK)
    {
        /* Handle error */
        const char *message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        fprintf(stderr, cass_fmt, (int)message_length, message);
        cass_future_free(connect_future);
        throw error;
    }
    cass_future_free(connect_future);
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
    }
    else
        check_and_throw(cass_cluster_set_contact_points(cluster, "127.0.0.1"),
                        "Actually it cannot fail.\n");

    /* Provide the cluster object as configuration to connect the session */
    check_and_throw(cass_session_connect(session, cluster), "Unable to connect: '%.*s'\n");
}

void read_version(CassSession *session);
void recreate_table(CassSession *session);

int main(int argc, char **argv)
{
    CassError error_code = CASS_OK;
    CassCluster *cluster = cass_cluster_new(); // I need it here to free it
    CassSession *session = cass_session_new(); // Session is used everywhere

    try
    {
        connect_cluster(argc, argv, cluster, session);

        read_version(session);
        std::cout << "Hello, world! Nice\n";
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

    std::cout << "Hello, world! Finally :)\n";

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

    cass_statement_free(statement);
    cass_future_free(result_future);
}

void recreate_table(CassSession *session)
{
    
}