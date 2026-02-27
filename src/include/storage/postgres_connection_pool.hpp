//===----------------------------------------------------------------------===//
//                         DuckDB
//
// storage/postgres_connection_pool.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/optional_ptr.hpp"
#include "postgres_connection.hpp"
#include <chrono>
#include <thread>
#include <condition_variable>
#include <atomic>

namespace duckdb {
class PostgresCatalog;
class PostgresConnectionPool;

class PostgresPoolConnection {
public:
	using time_point_t = std::chrono::steady_clock::time_point;

	PostgresPoolConnection();
	PostgresPoolConnection(optional_ptr<PostgresConnectionPool> pool, PostgresConnection connection,
	                       time_point_t created_at);
	~PostgresPoolConnection();
	// disable copy constructors
	PostgresPoolConnection(const PostgresPoolConnection &other) = delete;
	PostgresPoolConnection &operator=(const PostgresPoolConnection &) = delete;
	//! enable move constructors
	PostgresPoolConnection(PostgresPoolConnection &&other) noexcept;
	PostgresPoolConnection &operator=(PostgresPoolConnection &&) noexcept;

	bool HasConnection();
	PostgresConnection &GetConnection();

private:
	optional_ptr<PostgresConnectionPool> pool;
	PostgresConnection connection;
	time_point_t created_at;
};

class PostgresConnectionPool {
public:
	using steady_clock = std::chrono::steady_clock;
	using steady_time_point = steady_clock::time_point;

	static constexpr const idx_t DEFAULT_MAX_CONNECTIONS = 64;

	PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections = DEFAULT_MAX_CONNECTIONS);
	~PostgresConnectionPool();

public:
	bool TryGetConnection(PostgresPoolConnection &connection);
	PostgresPoolConnection GetConnection();
	//! Always returns a connection - even if the connection slots are exhausted
	PostgresPoolConnection ForceGetConnection();
	void ReturnConnection(PostgresConnection connection, steady_time_point created_at);
	void SetMaximumConnections(idx_t new_max);
	void SetMaxLifetime(idx_t seconds);
	void SetIdleTimeout(idx_t seconds);

	static void PostgresSetConnectionCache(ClientContext &context, SetScope scope, Value &parameter);

private:
	struct CachedConnection {
		PostgresConnection connection;
		steady_time_point created_at;
		steady_time_point returned_at;
	};

	PostgresCatalog &postgres_catalog;
	mutex connection_lock;
	idx_t active_connections;
	idx_t maximum_connections;
	vector<CachedConnection> connection_cache;

	idx_t max_lifetime_seconds = 0;
	idx_t idle_timeout_seconds = 0;

	std::thread reaper_thread;
	std::condition_variable reaper_cv;
	std::atomic<bool> shutdown {false};

	bool IsExpired(const CachedConnection &entry, steady_time_point now) const;
	void ReaperLoop();
	void StartReaperIfNeeded(unique_lock<mutex> &lock);
	void StopReaper(unique_lock<mutex> &lock);
	void UpdateTimeoutSetting(idx_t &field, idx_t seconds);

	PostgresPoolConnection GetConnectionInternal(unique_lock<mutex> &lock);
};

} // namespace duckdb
