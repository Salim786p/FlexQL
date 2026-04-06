#include "flexql.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>

#define MAX_SQL_LEN (4 * 1024 * 1024)

static int print_row(void *data, int ncols, char **values, char **col_names) {
    (void)data;
    for (int i = 0; i < ncols; ++i) {
        printf("%s = %s\n", col_names[i] ? col_names[i] : "(null)", values[i] ? values[i] : "NULL");
    }
    printf("\n");
    return 0;
}

static void print_help(void) {
    printf("FlexQL Interactive Terminal\n----------------------------\n");
    printf("  CREATE DATABASE name;\n  USE name;\n  SHOW DATABASES;\n  SHOW TABLES;\n  RESET table_name;\n");
    printf("  CREATE TABLE name (col TYPE [PRIMARY KEY] [NOT NULL], ...);\n");
    printf("  INSERT INTO name VALUES (v1, v2, ...);\n");
    printf("  SELECT * FROM t1 INNER JOIN t2 ON t1.col = t2.col WHERE col > 100 ORDER BY col DESC;\n");
    printf("  .help   – show this message\n  .exit   – quit\n\n");
}

int main(int argc, char *argv[]) {
    if (argc < 3) { fprintf(stderr, "Usage: %s <host> <port>\n", argv[0]); return 1; }
    const char *host = argv[1]; int port = atoi(argv[2]);
    FlexQL *db = NULL;
    if (flexql_open(host, port, &db) != FLEXQL_OK) { fprintf(stderr, "Cannot connect to server\n"); return 1; }

    printf("Connected to FlexQL server at %s:%d\nType .help for help, .exit to quit.\n\n", host, port);

    char *sql = (char *)malloc(MAX_SQL_LEN); size_t sql_len = 0;
    char line[4096];

    while (1) {
        printf(sql_len == 0 ? "flexql> " : "     -> "); fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) break;

        size_t line_len = strlen(line);
        while (line_len > 0 && (line[line_len-1] == '\n' || line[line_len-1] == '\r')) line[--line_len] = '\0';

        const char *trimmed = line; while (isspace((unsigned char)*trimmed)) ++trimmed;
        if (sql_len == 0 && trimmed[0] == '.') {
            if (strcmp(trimmed, ".exit") == 0 || strcmp(trimmed, ".quit") == 0) break;
            if (strcmp(trimmed, ".help") == 0) { print_help(); continue; }
            printf("Unknown command: %s  (try .help)\n", trimmed); continue;
        }

        if (line_len == 0 && sql_len == 0) continue;
        if (sql_len > 0) sql[sql_len++] = ' ';
        memcpy(sql + sql_len, line, line_len); sql_len += line_len; sql[sql_len] = '\0';

        const char *sq = sql + sql_len - 1; while (sq > sql && isspace((unsigned char)*sq)) --sq;
        if (*sq != ';') continue;

        char *errmsg = NULL;
        if (flexql_exec(db, sql, print_row, NULL, &errmsg) != FLEXQL_OK) {
            fprintf(stderr, "Error: %s\n", errmsg ? errmsg : "(unknown)"); flexql_free(errmsg);
        } else {
            char u[16] = {0}; strncpy(u, sql, 15);
            for(int i=0; i<15; i++) u[i] = toupper(u[i]);
            if (strncmp(u, "SELECT", 6) != 0 && strncmp(u, "SHOW", 4) != 0) printf("OK\n");
        }
        sql_len = 0; sql[0] = '\0';
    }
    free(sql); flexql_close(db);
    return 0;
}