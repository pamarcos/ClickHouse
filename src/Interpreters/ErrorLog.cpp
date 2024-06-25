#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeDateTime64.h>
#include <DataTypes/DataTypeLowCardinality.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <Interpreters/ErrorLog.h>
#include <base/getFQDNOrHostName.h>
#include <Common/DateLUTImpl.h>
#include <Common/ThreadPool.h>
#include <Common/ErrorCodes.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/parseQuery.h>

#include <algorithm>
#include <vector>

namespace DB
{

ColumnsDescription ErrorLogElement::getColumnsDescription()
{
    ParserCodec codec_parser;
    return ColumnsDescription {
        {
                "hostname",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Hostname of the server executing the query."
            },
        {
                "event_date",
                std::make_shared<DataTypeDate>(),
                parseQuery(codec_parser, "(Delta(2), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Event date."
            },
        {
                "event_time",
                std::make_shared<DataTypeDateTime>(),
                parseQuery(codec_parser, "(Delta(4), ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Event time."
            },
        {
                "code",
                std::make_shared<DataTypeInt32>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Error code."
            },
        {
                "error",
                std::make_shared<DataTypeLowCardinality>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Error name."
            },
        {
                "value",
                std::make_shared<DataTypeUInt64>(),
                parseQuery(codec_parser, "(ZSTD(3))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Number of errors happened in time interval."
            },
        {
                "remote",
                std::make_shared<DataTypeUInt8>(),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Remote exception (i.e. received during one of the distributed queries)."
            },
            {
                "query_ids",
                std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()),
                parseQuery(codec_parser, "(ZSTD(1))", 0, DBMS_DEFAULT_MAX_PARSER_DEPTH, DBMS_DEFAULT_MAX_PARSER_BACKTRACKS),
                "Last max_query_ids_in_system_errors query IDs that triggered this error."
            }
    };
}

void ErrorLogElement::appendToBlock(MutableColumns & columns) const
{
    size_t column_idx = 0;

    columns[column_idx++]->insert(getFQDNOrHostName());
    columns[column_idx++]->insert(DateLUT::instance().toDayNum(event_time).toUnderType());
    columns[column_idx++]->insert(event_time);
    columns[column_idx++]->insert(code);
    columns[column_idx++]->insert(ErrorCodes::getName(code));
    columns[column_idx++]->insert(value);
    columns[column_idx++]->insert(remote);
    columns[column_idx++]->insert(query_ids);
}

struct PreviousValue
{
    UInt64 count = 0;
    String last_query_id;
};

struct ValuePair
{
    PreviousValue local;
    PreviousValue remote;
};

Array getNewQueryIds(const std::deque<String> & query_ids, String query_id)
{
    Array new_query_ids;
    auto it = std::find(query_ids.begin(), query_ids.end(), query_id);
    if (it != query_ids.end())
        it++;

    while (it != query_ids.end())
    {
        new_query_ids.push_back(*it);
    }
    return new_query_ids;
}

void ErrorLog::stepFunction(TimePoint current_time)
{
    /// Static lazy initialization to avoid polluting the header with implementation details
    static std::vector<ValuePair> previous_values(ErrorCodes::end());

    auto event_time = std::chrono::system_clock::to_time_t(current_time);

    for (ErrorCodes::ErrorCode code = 0, end = ErrorCodes::end(); code < end; ++code)
    {
        const auto & error = ErrorCodes::values[code].get();
        if (error.local.count != previous_values.at(code).local.count)
        {
            ErrorLogElement local_elem {
                .event_time=event_time,
                .code=code,
                .value=error.local.count - previous_values.at(code).local.count,
                .remote=false,
                .query_ids=getNewQueryIds(error.local.query_ids, previous_values[code].local.last_query_id)
            };
            this->add(std::move(local_elem));
            previous_values[code].local.count = error.local.count;
            previous_values[code].local.last_query_id = error.local.query_ids.back();
        }
        if (error.remote.count != previous_values.at(code).remote.count)
        {
            ErrorLogElement remote_elem {
                .event_time=event_time,
                .code=code,
                .value=error.remote.count - previous_values.at(code).remote.count,
                .remote=true,
                .query_ids=getNewQueryIds(error.remote.query_ids, previous_values[code].remote.last_query_id)
            };
            this->add(std::move(remote_elem));
            previous_values[code].remote.count = error.remote.count;
            previous_values[code].remote.last_query_id = error.remote.query_ids.back();
        }
    }
}

}
