#include "duckdb/main/attached_database.hpp"
#include "duckdb/logging/file_system_logger.hpp"
#include "duckdb/logging/log_type.hpp"
#include "duckdb/common/file_opener.hpp"
#include "duckdb/common/http_util.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/execution/physical_operator.hpp"
#include "postgres_logging.hpp"

namespace duckdb {

constexpr LogLevel PostgresQueryLogType::LEVEL;

//===--------------------------------------------------------------------===//
// QueryLogType
//===--------------------------------------------------------------------===//
string PostgresQueryLogType::ConstructLogMessage(const string &str) {
        return str;
}

} // namespace
