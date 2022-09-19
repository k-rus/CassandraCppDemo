#include <cassandra.h>

#include <iostream>

CassCluster *connect_cluster(int argc, char **argv)
{
    CassCluster *cluster = cass_cluster_new();
    /* Add contact points */
    if (argc > 2)
    {
        if (cass_cluster_set_cloud_secure_connection_bundle(cluster, argv[1]) != CASS_OK)
            fprintf(stderr, "Unable to configure cloud using the secure connection bundle: %s\n",
                    argv[1]);
        else
            /* Set credentials provided when creating your database */
            cass_cluster_set_credentials(cluster, "token", argv[2]);
    }
    else
        cass_cluster_set_contact_points(cluster, "127.0.0.1");
    return cluster;
}

int main(int argc, char **argv)
{
    /* Setup and connect to cluster */
    CassFuture *connect_future = NULL;
    CassCluster *cluster = connect_cluster(argc, argv);
    CassSession *session = cass_session_new();

    /* Provide the cluster object as configuration to connect the session */
    connect_future = cass_session_connect(session, cluster);

    if (cass_future_error_code(connect_future) == CASS_OK)
    {
        CassFuture *close_future = NULL;

        /* Build statement and execute query */
        const char *query = "SELECT release_version FROM system.local";
        CassStatement *statement = cass_statement_new(query, 0);

        CassFuture *result_future = cass_session_execute(session, statement);

        if (cass_future_error_code(result_future) == CASS_OK)
        {
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
        }
        else
        {
            /* Handle error */
            const char *message;
            size_t message_length;
            cass_future_error_message(result_future, &message, &message_length);
            fprintf(stderr, "Unable to run query: '%.*s'\n", (int)message_length, message);
        }

        cass_statement_free(statement);
        cass_future_free(result_future);

        /* Close the session */
        close_future = cass_session_close(session);
        cass_future_wait(close_future);
        cass_future_free(close_future);
    }
    else
    {
        /* Handle error */
        const char *message;
        size_t message_length;
        cass_future_error_message(connect_future, &message, &message_length);
        fprintf(stderr, "Unable to connect: '%.*s'\n", (int)message_length, message);
    }

    cass_future_free(connect_future);
    cass_cluster_free(cluster);
    cass_session_free(session);

    std::cout << "Hello, world!\n";

    return 0;
}
