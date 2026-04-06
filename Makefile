# ============================================================
# FlexQL Makefile
# ============================================================

CXX      := g++
CC       := gcc
CXXFLAGS := -std=c++17 -O3 -Wall -Wextra -pthread
CFLAGS   := -O3 -Wall -Wextra -I.
LDFLAGS  := -pthread

INCLUDE  := -I.

BIN_DIR  := bin

SERVER_SRC  := flexql_server.cpp
CLIENT_SRC  := flexql_client.c
REPL_SRC    := repl.c

SERVER_BIN  := $(BIN_DIR)/flexql-server
CLIENT_BIN  := $(BIN_DIR)/flexql-client
CLIENT_LIB  := $(BIN_DIR)/libflexql.a
BENCH_SRC   := benchmark_flexql.cpp
BENCH_BIN   := $(BIN_DIR)/benchmark_flexql

.PHONY: all clean dirs

all: dirs $(SERVER_BIN) $(CLIENT_LIB) $(CLIENT_BIN) $(BENCH_BIN)

dirs:
	@mkdir -p $(BIN_DIR)
	@mkdir -p data/DEFAULT

# ---- Server ----
$(SERVER_BIN): $(SERVER_SRC) flexql.h
	$(CXX) $(CXXFLAGS) $(INCLUDE) $< -o $@

# ---- Client static library ----
$(BIN_DIR)/flexql_client.o: $(CLIENT_SRC) flexql.h
	$(CC) $(CFLAGS) -c $< -o $@

$(CLIENT_LIB): $(BIN_DIR)/flexql_client.o
	ar rcs $@ $^

# ---- REPL executable ----
$(BIN_DIR)/repl.o: $(REPL_SRC) flexql.h
	$(CC) $(CFLAGS) -c $< -o $@

$(CLIENT_BIN): $(BIN_DIR)/repl.o $(CLIENT_LIB)
	$(CC) $(CFLAGS) $^ -o $@

# ---- Benchmark executable linked against the client library ----
$(BENCH_BIN): $(BENCH_SRC) $(CLIENT_LIB) flexql.h
	$(CXX) $(CXXFLAGS) $(INCLUDE) $(BENCH_SRC) $(CLIENT_LIB) -o $@ $(LDFLAGS)

clean:
	rm -rf $(BIN_DIR)
	rm -rf data
