#include "storage/postgres_connection_pool.hpp"
#include "storage/postgres_catalog.hpp"
#include <algorithm>

namespace duckdb {
static bool pg_use_connection_cache = true;

PostgresPoolConnection::PostgresPoolConnection() : pool(nullptr), created_at() {
}

PostgresPoolConnection::PostgresPoolConnection(optional_ptr<PostgresConnectionPool> pool,
                                               PostgresConnection connection_p, time_point_t created_at_p)
    : pool(pool), connection(std::move(connection_p)), created_at(created_at_p) {
}

PostgresPoolConnection::~PostgresPoolConnection() {
	if (pool) {
		pool->ReturnConnection(std::move(connection), created_at);
	}
}

PostgresPoolConnection::PostgresPoolConnection(PostgresPoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
	std::swap(created_at, other.created_at);
}

PostgresPoolConnection &PostgresPoolConnection::operator=(PostgresPoolConnection &&other) noexcept {
	std::swap(pool, other.pool);
	std::swap(connection, other.connection);
	std::swap(created_at, other.created_at);
	return *this;
}

bool PostgresPoolConnection::HasConnection() {
	return pool;
}

PostgresConnection &PostgresPoolConnection::GetConnection() {
	if (!HasConnection()) {
		throw InternalException("PostgresPoolConnection::GetConnection called without a transaction pool");
	}
	return connection;
}

PostgresConnectionPool::PostgresConnectionPool(PostgresCatalog &postgres_catalog, idx_t maximum_connections_p)
    : postgres_catalog(postgres_catalog), active_connections(0), maximum_connections(maximum_connections_p) {
}

PostgresConnectionPool::~PostgresConnectionPool() {
	unique_lock<mutex> l(connection_lock);
	StopReaper(l);
}

bool PostgresConnectionPool::IsExpired(const CachedConnection &entry, steady_time_point now) const {
	if (max_lifetime_seconds > 0) {
		auto age = std::chrono::duration_cast<std::chrono::seconds>(now - entry.created_at).count();
		if (static_cast<idx_t>(age) >= max_lifetime_seconds) {
			return true;
		}
	}
	if (idle_timeout_seconds > 0) {
		auto idle = std::chrono::duration_cast<std::chrono::seconds>(now - entry.returned_at).count();
		if (static_cast<idx_t>(idle) >= idle_timeout_seconds) {
			return true;
		}
	}
	return false;
}

void PostgresConnectionPool::ReaperLoop() {
	unique_lock<mutex> l(connection_lock);
	while (!shutdown.load()) {
		idx_t sleep_seconds = 30;
		if (max_lifetime_seconds > 0 && idle_timeout_seconds > 0) {
			sleep_seconds = std::min(max_lifetime_seconds, idle_timeout_seconds);
		} else if (max_lifetime_seconds > 0) {
			sleep_seconds = max_lifetime_seconds;
		} else if (idle_timeout_seconds > 0) {
			sleep_seconds = idle_timeout_seconds;
		}
		sleep_seconds = std::max<idx_t>(1, sleep_seconds / 2);
		sleep_seconds = std::min<idx_t>(60, sleep_seconds);

		reaper_cv.wait_for(l, std::chrono::seconds(sleep_seconds), [this]() { return shutdown.load(); });

		if (shutdown.load()) {
			break;
		}

		auto now = steady_clock::now();
		auto it = std::partition(connection_cache.begin(), connection_cache.end(),
		                         [this, now](const CachedConnection &e) { return !IsExpired(e, now); });
		vector<CachedConnection> expired(std::make_move_iterator(it), std::make_move_iterator(connection_cache.end()));
		connection_cache.erase(it, connection_cache.end());
		// release lock while destroying expired connections (PQfinish may block)
		l.unlock();
		expired.clear();
		l.lock();
	}
}

void PostgresConnectionPool::StartReaperIfNeeded(unique_lock<mutex> &lock) {
	if (max_lifetime_seconds == 0 && idle_timeout_seconds == 0) {
		return;
	}
	if (reaper_thread.joinable()) {
		return;
	}
	shutdown.store(false);
	reaper_thread = std::thread(&PostgresConnectionPool::ReaperLoop, this);
}

void PostgresConnectionPool::StopReaper(unique_lock<mutex> &lock) {
	if (!reaper_thread.joinable()) {
		return;
	}
	shutdown.store(true);
	reaper_cv.notify_all();
	lock.unlock();
	reaper_thread.join();
	lock.lock();
}

PostgresPoolConnection PostgresConnectionPool::GetConnectionInternal(unique_lock<mutex> &lock) {
	active_connections++;
	auto now = steady_clock::now();
	while (!connection_cache.empty()) {
		auto cached = std::move(connection_cache.back());
		connection_cache.pop_back();
		if (IsExpired(cached, now)) {
			continue;
		}
		return PostgresPoolConnection(this, std::move(cached.connection), cached.created_at);
	}
	lock.unlock();
	auto created = steady_clock::now();
	try {
		return PostgresPoolConnection(
		    this, PostgresConnection::Open(postgres_catalog.connection_string, postgres_catalog.attach_path), created);
	} catch (...) {
		lock.lock();
		active_connections--;
		throw;
	}
}

PostgresPoolConnection PostgresConnectionPool::ForceGetConnection() {
	unique_lock<mutex> l(connection_lock);
	return GetConnectionInternal(l);
}

bool PostgresConnectionPool::TryGetConnection(PostgresPoolConnection &connection) {
	unique_lock<mutex> l(connection_lock);
	if (active_connections >= maximum_connections) {
		return false;
	}
	connection = GetConnectionInternal(l);
	return true;
}

void PostgresConnectionPool::PostgresSetConnectionCache(ClientContext &context, SetScope scope, Value &parameter) {
	if (parameter.IsNull()) {
		throw BinderException("Cannot be set to NULL");
	}
	pg_use_connection_cache = BooleanValue::Get(parameter);
}

PostgresPoolConnection PostgresConnectionPool::GetConnection() {
	PostgresPoolConnection result;
	if (!TryGetConnection(result)) {
		throw IOException(
		    "Failed to get connection from PostgresConnectionPool - maximum connection count exceeded (%llu/%llu max)",
		    active_connections, maximum_connections);
	}
	return result;
}

void PostgresConnectionPool::ReturnConnection(PostgresConnection connection, steady_time_point created_at) {
	unique_lock<mutex> l(connection_lock);
	if (active_connections <= 0) {
		throw InternalException("PostgresConnectionPool::ReturnConnection called but active_connections is 0");
	}
	if (!pg_use_connection_cache) {
		// not caching - just return
		active_connections--;
		return;
	}

	// check if the connection has exceeded its max lifetime before doing anything else
	if (max_lifetime_seconds > 0) {
		auto age = std::chrono::duration_cast<std::chrono::seconds>(steady_clock::now() - created_at).count();
		if (static_cast<idx_t>(age) >= max_lifetime_seconds) {
			active_connections--;
			return;
		}
	}

	// we want to cache the connection
	// check if the underlying connection is still usable
	// avoid holding the lock while doing this
	l.unlock();
	bool connection_is_bad = false;
	auto pg_con = connection.GetConn();
	if (PQstatus(connection.GetConn()) != CONNECTION_OK) {
		// CONNECTION_BAD! try to reset it
		PQreset(pg_con);
		if (PQstatus(connection.GetConn()) != CONNECTION_OK) {
			// still bad - just abandon this one
			connection_is_bad = true;
		}
	}
	if (!connection_is_bad && PQtransactionStatus(pg_con) != PQTRANS_IDLE) {
		connection_is_bad = true;
	}

	// lock and return the connection
	l.lock();
	active_connections--;
	if (connection_is_bad) {
		// if the connection is bad we cannot cache it
		return;
	}
	if (active_connections >= maximum_connections) {
		// if the maximum number of connections has been decreased by the user we might need to reclaim the connection
		// immediately
		return;
	}
	CachedConnection cached;
	cached.connection = std::move(connection);
	cached.created_at = created_at;
	cached.returned_at = steady_clock::now();
	connection_cache.push_back(std::move(cached));
}

void PostgresConnectionPool::SetMaximumConnections(idx_t new_max) {
	lock_guard<mutex> l(connection_lock);
	if (new_max < maximum_connections) {
		// potentially close connections
		// note that we can only close connections in the connection cache
		// we will have to wait for connections to be returned
		auto total_open_connections = active_connections + connection_cache.size();
		while (!connection_cache.empty() && total_open_connections > new_max) {
			total_open_connections--;
			connection_cache.pop_back();
		}
	}
	maximum_connections = new_max;
}

void PostgresConnectionPool::UpdateTimeoutSetting(idx_t &field, idx_t seconds) {
	unique_lock<mutex> l(connection_lock);
	field = seconds;
	if (max_lifetime_seconds == 0 && idle_timeout_seconds == 0) {
		StopReaper(l);
	} else {
		StartReaperIfNeeded(l);
		reaper_cv.notify_all();
	}
}

void PostgresConnectionPool::SetMaxLifetime(idx_t seconds) {
	UpdateTimeoutSetting(max_lifetime_seconds, seconds);
}

void PostgresConnectionPool::SetIdleTimeout(idx_t seconds) {
	UpdateTimeoutSetting(idle_timeout_seconds, seconds);
}

} // namespace duckdb
