#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeArray.h>
#include <Storages/System/StorageSystemErrors.h>
#include <Interpreters/Context.h>
#include "Common/CurrentThread.h"
#include <Common/ErrorCodes.h>


namespace DB
{

ColumnsDescription StorageSystemErrors::getColumnsDescription()
{
    return ColumnsDescription
    {
        { "name",                    std::make_shared<DataTypeString>(), "Name of the error (errorCodeToName)."},
        { "code",                    std::make_shared<DataTypeInt32>(), "Code number of the error."},
        { "value",                   std::make_shared<DataTypeUInt64>(), "The number of times this error happened."},
        { "last_error_time",         std::make_shared<DataTypeDateTime>(), "The time when the last error happened."},
        { "last_error_message",      std::make_shared<DataTypeString>(), "Message for the last error."},
        { "last_error_trace",        std::make_shared<DataTypeArray>(std::make_shared<DataTypeUInt64>()), "A stack trace that represents a list of physical addresses where the called methods are stored."},
        { "remote",                  std::make_shared<DataTypeUInt8>(), "Remote exception (i.e. received during one of the distributed queries)."},
        { "query_ids",               std::make_shared<DataTypeArray>(std::make_shared<DataTypeString>()), "Last max_query_ids_in_system_errors query IDs that triggered this error."},
    };
}


void StorageSystemErrors::fillData(MutableColumns & res_columns, ContextPtr context, const ActionsDAG::Node *, std::vector<UInt8>) const
{
    auto add_row = [&](std::string_view name, size_t code, const auto & error, bool remote)
    {
        if (error.count || context->getSettingsRef().system_events_show_zero_values)
        {
            size_t col_num = 0;
            res_columns[col_num++]->insert(name);
            res_columns[col_num++]->insert(code);
            res_columns[col_num++]->insert(error.count);
            res_columns[col_num++]->insert(error.error_time_ms / 1000);
            res_columns[col_num++]->insert(error.message);
            {
                Array trace_array;
                trace_array.reserve(error.trace.size());
                for (size_t i = 0; i < error.trace.size(); ++i)
                    trace_array.emplace_back(reinterpret_cast<intptr_t>(error.trace[i]));

                res_columns[col_num++]->insert(trace_array);
            }
            res_columns[col_num++]->insert(remote);
            {
                Array query_ids_array;
                query_ids_array.reserve(error.query_ids.size());
                for (const auto & query_id : error.query_ids)
                {
                    query_ids_array.emplace_back(query_id);
                }
                res_columns[col_num++]->insert(query_ids_array);
            }
        }
    };

    for (size_t i = 0, end = ErrorCodes::end(); i < end; ++i)
    {
        const auto & error = ErrorCodes::values[i].get();
        std::string_view name = ErrorCodes::getName(static_cast<ErrorCodes::ErrorCode>(i));

        if (name.empty())
            continue;

        add_row(name, i, error.local,  /* remote= */ false);
        add_row(name, i, error.remote, /* remote= */ true);
    }
}

}
