#include <stdio.h>
#include <string.h>
#include <memory.h>
#include <mysql.h>

#define QUERIES_SIZE 14

char *queries[QUERIES_SIZE] =
  {
   "create table test (pk int, `value` int, primary key(pk))",
   "describe test",
   "select * from test",
   "insert into test (pk, `value`) values (0,0)",
   "select * from test",
   "call dolt_add('-A');",
   "call dolt_commit('-m', 'my commit')",
   "select COUNT(*) FROM dolt_log",
   "call dolt_checkout('-b', 'mybranch')",
   "insert into test (pk, `value`) values (10,10)",
   "call dolt_commit('-a', '-m', 'my commit2')",
   "call dolt_checkout('main')",
   "call dolt_merge('mybranch')",
   "select COUNT(*) FROM dolt_log",
  };

typedef struct festate_t {
  char *query;
  MYSQL_STMT* stmt;
  MYSQL_BIND mysql_bind[10];
} festate;

void GetForeignRelSize() {

}

void BeginForeignScan(MYSQL* conn, festate* state) {
    fprintf(stderr, "===BEGIN FOREIGN SCAN===\n");

    // set sql_mode
    if (mysql_query(conn, "SET sql_mode = 'ANSI_QUOTES'")) {
        fprintf(stderr, "failed to set sql_mode: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "sql_mode set successfully\n");

    // initialize mysql statement
    state->stmt = mysql_stmt_init(conn);
    if (!state->stmt) {
        fprintf(stderr, "failed to initialize the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "mysql query initialized successfully\n");

    // prepare the mysql statement
    if (mysql_stmt_prepare(state->stmt, state->query, strlen(state->query))) {
        fprintf(stderr, "failed to prepare the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "mysql query prepared successfully\n");

    //

    // TODO: Prepare for output conversion of parameters used in remote query
    // TODO: Set the statement as cursor type
    // TODO: Set the pre-fetch rows
    // TODO: Set STMT_ATTR_UPDATE_MAX_LENGTH so that mysql_stmt_store_result() can update metadata MYSQL_FIELD->max_length value

    // Bind the results pointers for the prepare statements
    if (mysql_stmt_bind_result(state->stmt, state->mysql_bind)) {
        fprintf(stderr, "failed to bind the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "mysql query bound successfully\n");
}

void IterateForeignScan(MYSQL* conn, festate* state) {
    fprintf(stderr, "===ITERATE FOREIGN SCAN===\n");
    // TODO: assume this is the first call
    // TODO: if num params > 0, mysql_stmt_bind_param

    if (mysql_stmt_execute(state->stmt)) {
        fprintf(stderr, "failed to execute the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "mysql query executed successfully\n");

//    if (mysql_stmt_store_result(state->stmt) != 0) {
//        fprintf(stderr, "failed to store the result: \n%s", mysql_error(conn));
//        exit(1);
//    }
//    fprintf(stderr, "results stored successfully\n");

    // Bind the results pointers for the prepare statements
    if (mysql_stmt_bind_result(state->stmt, state->mysql_bind)) {
        fprintf(stderr, "failed to bind the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "results bound successfully\n");

    int rc = mysql_stmt_fetch(state->stmt);
    if (rc == 1) {
        fprintf(stderr, "failed to fetch the mysql query: \n%s", mysql_error(conn));
        exit(1);
    }
    if (rc == MYSQL_NO_DATA) {
        fprintf(stderr, "fetched empty result set\n");
        exit(1);
    }
    if (rc == MYSQL_DATA_TRUNCATED) {
        fprintf(stderr, "mysql data truncated\n");
        exit(1);
    }

    // set sql_mode
    if (mysql_query(conn, "SET sql_mode = 'ANSI_QUOTES'")) {
        fprintf(stderr, "failed to set sql_mode: \n%s", mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "sql_mode set successfully\n");

    fprintf(stderr, "results fetched successfully\n");
}

int main(int argc, char **argv) {
    char* user = argv[1];
    int   port = atoi(argv[2]);
    char* db   = argv[3];

    MYSQL *conn = mysql_init(NULL);
    if (conn == NULL) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        exit(1);
    }

    if (mysql_real_connect(conn, "127.0.0.1", user, "root", db, port, NULL, 0) == NULL) {
        fprintf(stderr, "%s\n", mysql_error(conn));
        mysql_close(conn);
        exit(1);
    }

    int pk = 1;
    festate state = {
        .query = "SELECT `i` from `test_db`.`t`",
        .mysql_bind = {
            [0] = {
                .buffer_type = MYSQL_TYPE_LONG,
                .buffer = (void *)(&pk),
                .buffer_length = sizeof(pk),
            }
        },
    };

    BeginForeignScan(conn, &state);
    IterateForeignScan(conn, &state);
    if ( mysql_stmt_close(state.stmt) ) {
        fprintf(stderr, "failed to close stmt: %s: %s\n", state.query, mysql_error(conn));
        exit(1);
    }
    fprintf(stderr, "statement closed successfully\n");

    mysql_close(conn);
    return 0;
}
