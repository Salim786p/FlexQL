#define _POSIX_C_SOURCE 200112L
#define _GNU_SOURCE

#include "flexql.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <ctype.h>
#include <time.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

struct FlexQL {
    int  sockfd;
    char host[256];
    int  port;
    char *resp_buf;
    size_t resp_cap;
    struct {
        char *data;
        size_t len;
        size_t cap;
    } req_buf;
};

enum {
    BINPROTO_MAGIC  = 0x01,
    BINPROTO_INSERT = 0x01,
    BINPROTO_ACK    = 0x02,
    BINPROTO_ERROR  = 0x03
};

typedef struct {
    char *data;
    size_t len;
    size_t cap;
} ByteBuffer;

static int send_all(int fd, const char *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, buf + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) return -1;
        sent += (size_t)n;
    }
    return 0;
}

static int recv_all(int fd, char *buf, size_t len) {
    size_t got = 0;
    while (got < len) {
        ssize_t n = recv(fd, buf + got, len - got, 0);
        if (n <= 0) return -1;
        got += (size_t)n;
    }
    return 0;
}

static int send_msg(int fd, const char *msg, size_t len) {
    uint32_t net_len = htonl((uint32_t)len);
    struct iovec iov[2];
    iov[0].iov_base = (void *)&net_len;
    iov[0].iov_len = sizeof(net_len);
    iov[1].iov_base = (void *)msg;
    iov[1].iov_len = len;
    struct iovec pending[2];
    pending[0] = iov[0];
    pending[1] = iov[1];
    size_t idx = 0;
    while (idx < 2) {
        ssize_t n = writev(fd, pending + idx, (int)(2 - idx));
        if (n <= 0) return -1;
        size_t consumed = (size_t)n;
        while (idx < 2 && consumed >= pending[idx].iov_len) {
            consumed -= pending[idx].iov_len;
            ++idx;
        }
        if (idx < 2 && consumed > 0) {
            pending[idx].iov_base = (char *)pending[idx].iov_base + consumed;
            pending[idx].iov_len -= consumed;
        }
    }
    return 0;
}

static int recv_msg(FlexQL *db, char **out, size_t *out_len) {
    uint32_t net_len;
    if (recv_all(db->sockfd, (char *)&net_len, 4) != 0) return -1;
    uint32_t len = ntohl(net_len);
    if (len == 0) {
        if (db->resp_cap < 1) {
            char *buf = (char *)realloc(db->resp_buf, 1);
            if (!buf) return -1;
            db->resp_buf = buf;
            db->resp_cap = 1;
        }
        db->resp_buf[0] = '\0';
        *out = db->resp_buf;
        *out_len = 0;
        return 0;
    }
    if (len > 256 * 1024 * 1024u) return -1; 
    if (db->resp_cap < (size_t)len + 1) {
        char *buf = (char *)realloc(db->resp_buf, (size_t)len + 1);
        if (!buf) return -1;
        db->resp_buf = buf;
        db->resp_cap = (size_t)len + 1;
    }
    db->resp_buf[len] = '\0';
    if (recv_all(db->sockfd, db->resp_buf, len) != 0) return -1;
    *out = db->resp_buf;
    *out_len = len;
    return 0;
}

static void buf_free(ByteBuffer *buf) {
    free(buf->data);
    buf->data = NULL;
    buf->len = 0;
    buf->cap = 0;
}

static int buf_reserve(ByteBuffer *buf, size_t extra) {
    size_t need = buf->len + extra;
    if (need <= buf->cap) return 0;
    size_t cap = buf->cap ? buf->cap : 256;
    while (cap < need) cap += cap >> 1;
    char *data = (char *)realloc(buf->data, cap);
    if (!data) return -1;
    buf->data = data;
    buf->cap = cap;
    return 0;
}

static int buf_append_bytes(ByteBuffer *buf, const void *data, size_t len) {
    if (buf_reserve(buf, len) != 0) return -1;
    memcpy(buf->data + buf->len, data, len);
    buf->len += len;
    return 0;
}

static int buf_append_u8(ByteBuffer *buf, uint8_t v) { return buf_append_bytes(buf, &v, 1); }
static int buf_append_u32(ByteBuffer *buf, uint32_t v) {
    uint32_t net = htonl(v);
    return buf_append_bytes(buf, &net, 4);
}
static int buf_append_i64_be(ByteBuffer *buf, int64_t v) {
    unsigned char tmp[8];
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; ++i) {
        tmp[7 - i] = (unsigned char)(u & 0xFFu);
        u >>= 8;
    }
    return buf_append_bytes(buf, tmp, 8);
}
static void buf_patch_u32(ByteBuffer *buf, size_t pos, uint32_t v) {
    uint32_t net = htonl(v);
    memcpy(buf->data + pos, &net, 4);
}
static void buf_patch_i64_be(ByteBuffer *buf, size_t pos, int64_t v) {
    uint64_t u = (uint64_t)v;
    for (int i = 0; i < 8; ++i) {
        buf->data[pos + 7 - i] = (char)(u & 0xFFu);
        u >>= 8;
    }
}

static void skip_ws(const char *sql, size_t len, size_t *pos) {
    while (*pos < len && isspace((unsigned char)sql[*pos])) ++(*pos);
}
static int consume_keyword_ci(const char *sql, size_t len, size_t *pos, const char *kw) {
    skip_ws(sql, len, pos);
    size_t start = *pos;
    for (size_t i = 0; kw[i] != '\0'; ++i) {
        if (*pos >= len || toupper((unsigned char)sql[*pos]) != kw[i]) {
            *pos = start;
            return 0;
        }
        ++(*pos);
    }
    if (*pos < len && (isalnum((unsigned char)sql[*pos]) || sql[*pos] == '_')) {
        *pos = start;
        return 0;
    }
    return 1;
}
static int consume_char(const char *sql, size_t len, size_t *pos, char ch) {
    skip_ws(sql, len, pos);
    if (*pos >= len || sql[*pos] != ch) return 0;
    ++(*pos);
    return 1;
}
static int append_ident_upper(const char *sql, size_t len, size_t *pos, ByteBuffer *buf) {
    skip_ws(sql, len, pos);
    if (*pos >= len) return 0;
    unsigned char ch = (unsigned char)sql[*pos];
    if (!isalpha(ch) && ch != '_') return 0;
    size_t start = (*pos)++;
    while (*pos < len) {
        unsigned char c = (unsigned char)sql[*pos];
        if (!isalnum(c) && c != '_') break;
        ++(*pos);
    }
    size_t n = *pos - start;
    if (buf_append_u32(buf, (uint32_t)n) != 0) return 0;
    if (buf_reserve(buf, n) != 0) return 0;
    for (size_t i = 0; i < n; ++i)
        buf->data[buf->len + i] = (char)toupper((unsigned char)sql[start + i]);
    buf->len += n;
    return 1;
}
static int append_scalar(ByteBuffer *buf, const char *sql, size_t len, size_t *pos) {
    skip_ws(sql, len, pos);
    if (*pos >= len) return 0;
    size_t len_pos = buf->len;
    if (buf_append_u32(buf, 0) != 0) return 0;
    uint32_t vlen = 0;
    if (sql[*pos] == '\'' || sql[*pos] == '"') {
        char quote = sql[(*pos)++];
        while (*pos < len) {
            char c = sql[(*pos)++];
            if (c == quote) {
                buf_patch_u32(buf, len_pos, vlen);
                return 1;
            }
            if (c == '\\' && *pos < len) c = sql[(*pos)++];
            if (buf_append_bytes(buf, &c, 1) != 0) return 0;
            ++vlen;
        }
        return 0;
    }
    size_t start = *pos;
    while (*pos < len) {
        char c = sql[*pos];
        if (c == ',' || c == ')' || c == ';' || isspace((unsigned char)c)) break;
        ++(*pos);
    }
    if (start == *pos) return 0;
    vlen = (uint32_t)(*pos - start);
    if (buf_append_bytes(buf, sql + start, vlen) != 0) return 0;
    buf_patch_u32(buf, len_pos, vlen);
    return 1;
}
static int build_binary_insert_request(const char *sql, ByteBuffer *out) {
    size_t len = strlen(sql), pos = 0;
    uint32_t rows = 0, cols = 0;
    size_t cols_pos, rows_pos, exp_pos;
    out->len = 0;
    if (!consume_keyword_ci(sql, len, &pos, "INSERT")) return 0;
    if (!consume_keyword_ci(sql, len, &pos, "INTO")) return 0;
    if (buf_append_u8(out, BINPROTO_MAGIC) != 0 || buf_append_u8(out, BINPROTO_INSERT) != 0) return 0;
    if (!append_ident_upper(sql, len, &pos, out)) return 0;
    if (!consume_keyword_ci(sql, len, &pos, "VALUES")) return 0;
    cols_pos = out->len; if (buf_append_u32(out, 0) != 0) return 0;
    rows_pos = out->len; if (buf_append_u32(out, 0) != 0) return 0;
    exp_pos = out->len; if (buf_append_i64_be(out, 0) != 0) return 0;
    while (1) {
        uint32_t row_cols = 0;
        if (!consume_char(sql, len, &pos, '(')) return 0;
        while (1) {
            if (!append_scalar(out, sql, len, &pos)) return 0;
            ++row_cols;
            skip_ws(sql, len, &pos);
            if (pos >= len) return 0;
            if (sql[pos] == ')') { ++pos; break; }
            if (sql[pos] != ',') return 0;
            ++pos;
        }
        if (rows == 0) cols = row_cols;
        else if (row_cols != cols) return 0;
        ++rows;
        skip_ws(sql, len, &pos);
        if (pos >= len || sql[pos] != ',') break;
        ++pos;
    }
    skip_ws(sql, len, &pos);
    if (pos < len && sql[pos] == ';') ++pos;
    skip_ws(sql, len, &pos);
    if (pos != len || rows == 0) return 0;
    buf_patch_u32(out, cols_pos, cols);
    buf_patch_u32(out, rows_pos, rows);
    buf_patch_i64_be(out, exp_pos, 0);
    return 1;
}

static char *unescape_value(const char *s, size_t len) {
    char *out = (char *)malloc(len + 1);
    if (!out) return NULL;
    size_t i = 0, j = 0;
    while (i < len) {
        if (s[i] == '\\' && i + 1 < len) {
            ++i;
            switch (s[i]) {
                case '|':  out[j++] = '|';  break;
                case 'n':  out[j++] = '\n'; break;
                case '\\': out[j++] = '\\'; break;
                default:   out[j++] = s[i]; break;
            }
        } else {
            out[j++] = s[i];
        }
        ++i;
    }
    out[j] = '\0';
    return out;
}

static int parse_result_and_callback(const char *response, size_t resp_len, int (*callback)(void *, int, char **, char **), void *arg, char **errmsg) {
    char *buf = (char *)malloc(resp_len + 1);
    if (!buf) { 
        if (errmsg) *errmsg = strdup("Out of memory"); 
        return FLEXQL_ERROR; 
    }
    memcpy(buf, response, resp_len); 
    buf[resp_len] = '\0';
    
    char *p = buf; 
    char *end = buf + resp_len; 
    int rc = FLEXQL_OK;

#define NEXT_LINE(lineptr, linelen) do { \
        char *nl = (char *)memchr(p, '\n', (size_t)(end - p)); \
        if (!nl) { rc = FLEXQL_ERROR; if (errmsg) *errmsg = strdup("Truncated response"); goto done; } \
        *nl = '\0'; lineptr = p; linelen = (size_t)(nl - p); p = nl + 1; \
    } while (0)

    char *line; size_t llen;
    NEXT_LINE(line, llen);
    if (strcmp(line, "RESULT") != 0) {
        if (errmsg) {
            *errmsg = strdup("Expected RESULT header"); 
        }
        rc = FLEXQL_ERROR; 
        goto done;
    }

    NEXT_LINE(line, llen);
    int num_cols = atoi(line);
    if (num_cols <= 0) { 
        if (errmsg) {
            *errmsg = strdup("Invalid column count"); 
        }
        rc = FLEXQL_ERROR; 
        goto done; 
    }

    char **col_names = (char **)calloc((size_t)num_cols, sizeof(char *));
    for (int i = 0; i < num_cols; ++i) { 
        NEXT_LINE(line, llen); 
        col_names[i] = unescape_value(line, llen); 
    }

    NEXT_LINE(line, llen);
    int num_rows = atoi(line);
    char **values = (char **)calloc((size_t)num_cols, sizeof(char *));

    for (int row = 0; row < num_rows; ++row) {
        NEXT_LINE(line, llen);
        size_t col_start = 0; int ci = 0;
        for (size_t k = 0; k <= llen && ci < num_cols; ++k) {
            int is_end = (k == llen);
            int is_sep = (!is_end && line[k] == '|' && (k == 0 || line[k-1] != '\\'));
            if (is_end || is_sep) {
                values[ci++] = unescape_value(line + col_start, k - col_start); col_start = k + 1;
            }
        }
        while (ci < num_cols) values[ci++] = strdup("NULL");
        if (callback && callback(arg, num_cols, values, col_names) != 0) {
            for (int i = 0; i < num_cols; ++i) {
                free(values[i]); 
            }
            break;
        }
        for (int i = 0; i < num_cols; ++i) { 
            free(values[i]); 
            values[i] = NULL; 
        }
    }
    free(values);
    for (int i = 0; i < num_cols; ++i) {
        free(col_names[i]); 
    }
    free(col_names);
done:
    free(buf); 
    return rc;
#undef NEXT_LINE
}

int flexql_open(const char *host, int port, FlexQL **db) {
    if (!host || port <= 0 || port > 65535 || !db) return FLEXQL_ERROR;
    struct addrinfo hints, *res;
    memset(&hints, 0, sizeof(hints)); hints.ai_family = AF_INET; hints.ai_socktype = SOCK_STREAM;
    char port_str[16]; snprintf(port_str, sizeof(port_str), "%d", port);
    if (getaddrinfo(host, port_str, &hints, &res) != 0) return FLEXQL_ERROR;

    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { freeaddrinfo(res); return FLEXQL_ERROR; }
    int one = 1;
    setsockopt(sockfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
    if (connect(sockfd, res->ai_addr, res->ai_addrlen) < 0) {
        close(sockfd); freeaddrinfo(res); return FLEXQL_ERROR;
    }
    freeaddrinfo(res);

    FlexQL *handle = (FlexQL *)calloc(1, sizeof(FlexQL));
    if (!handle) { close(sockfd); return FLEXQL_ERROR; }
    handle->sockfd = sockfd; handle->port = port;
    handle->resp_buf = NULL; handle->resp_cap = 0;
    handle->req_buf.data = NULL; handle->req_buf.len = 0; handle->req_buf.cap = 0;
    strncpy(handle->host, host, 255); handle->host[255] = '\0';
    *db = handle; return FLEXQL_OK;
}

int flexql_close(FlexQL *db) {
    if (!db) return FLEXQL_ERROR;
    close(db->sockfd);
    free(db->resp_buf);
    buf_free((ByteBuffer *)&db->req_buf);
    free(db);
    return FLEXQL_OK;
}

int flexql_exec(FlexQL *db, const char *sql, int (*callback)(void *, int, char **, char **), void *arg, char **errmsg) {
    if (!db || !sql) { 
        if (errmsg) *errmsg = strdup("Invalid handle or SQL"); 
        return FLEXQL_ERROR; 
    }
    
    ByteBuffer *req = (ByteBuffer *)&db->req_buf;
    int use_binary_insert = build_binary_insert_request(sql, req);
    if ((use_binary_insert && send_msg(db->sockfd, req->data, req->len) != 0) ||
        (!use_binary_insert && send_msg(db->sockfd, sql, strlen(sql)) != 0)) {
        if (errmsg) {
            *errmsg = strdup("Failed to send query"); 
        }
        return FLEXQL_ERROR;
    }
    
    char *response = NULL; size_t resp_len = 0;
    
    if (recv_msg(db, &response, &resp_len) != 0) {
        if (errmsg) {
            *errmsg = strdup("Failed to receive response"); 
        }
        return FLEXQL_ERROR;
    }
    
    int rc = FLEXQL_OK;
    if (use_binary_insert && resp_len >= 2 && (unsigned char)response[0] == BINPROTO_MAGIC) {
        unsigned char opcode = (unsigned char)response[1];
        if (opcode == BINPROTO_ACK) {
        } else if (opcode == BINPROTO_ERROR) {
            rc = FLEXQL_ERROR;
            if (errmsg && resp_len >= 6) {
                uint32_t net_len = 0, msg_len = 0;
                memcpy(&net_len, response + 2, 4);
                msg_len = ntohl(net_len);
                if ((size_t)msg_len <= resp_len - 6) {
                    char *msg = (char *)malloc((size_t)msg_len + 1);
                    if (msg) {
                        memcpy(msg, response + 6, msg_len);
                        msg[msg_len] = '\0';
                        *errmsg = msg;
                    }
                }
            }
            if (errmsg && !*errmsg) *errmsg = strdup("Binary insert failed");
        } else {
            rc = FLEXQL_ERROR;
            if (errmsg) *errmsg = strdup("Unknown binary response");
        }
    } else if (strncmp(response, "OK\n", 3) == 0) {
        // Success
    } else if (strncmp(response, "ERROR\n", 6) == 0) {
        rc = FLEXQL_ERROR;
        if (errmsg) {
            const char *msg_start = response + 6; size_t msg_len = resp_len - 6;
            while (msg_len > 0 && msg_start[msg_len-1] == '\n') --msg_len;
            char *msg = (char *)malloc(msg_len + 1);
            if (msg) { memcpy(msg, msg_start, msg_len); msg[msg_len] = '\0'; *errmsg = msg; }
        }
    } else if (strncmp(response, "RESULT\n", 7) == 0) {
        rc = parse_result_and_callback(response, resp_len, callback, arg, errmsg);
    } else {
        rc = FLEXQL_ERROR; 
        if (errmsg) *errmsg = strdup("Unknown response format");
    }
    return rc;
}

void flexql_free(void *ptr) { free(ptr); }
