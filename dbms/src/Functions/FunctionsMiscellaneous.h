// Copyright 2022 PingCAP, Ltd.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <Columns/ColumnFunction.h>
#include <Columns/ColumnArray.h>
#include <Columns/ColumnConst.h>
#include <Columns/ColumnSet.h>
#include <Columns/ColumnString.h>
#include <Columns/ColumnTuple.h>
#include <DataTypes/DataTypeFunction.h>
#include <Functions/IFunction.h>
#include <IO/Operators.h>
#include <IO/WriteBufferFromString.h>
#include <Interpreters/ExpressionActions.h>

#include <Common/typeid_cast.h>
#include <DataTypes/DataTypeAggregateFunction.h>
#include <DataTypes/DataTypeArray.h>
#include <DataTypes/DataTypeDate.h>
#include <DataTypes/DataTypeDateTime.h>
#include <DataTypes/DataTypeEnum.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeSet.h>
#include <DataTypes/DataTypeString.h>
#include <DataTypes/DataTypeTuple.h>
#include <DataTypes/DataTypesNumber.h>
#include <DataTypes/NumberTraits.h>
#include <Functions/FunctionFactory.h>
#include <Functions/FunctionHelpers.h>
#include <Interpreters/Context.h>
#include <Interpreters/Set.h>

namespace DB
{
namespace ErrorCodes
{
extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
extern const int FUNCTION_IS_SPECIAL;
extern const int ARGUMENT_OUT_OF_BOUND;
extern const int TOO_SLOW;
extern const int ILLEGAL_COLUMN;
extern const int ILLEGAL_TYPE_OF_ARGUMENT;
extern const int FUNCTION_THROW_IF_VALUE_IS_NON_ZERO;
} // namespace ErrorCodes
/** Creates an array, multiplying the column (the first argument) by the number of elements in the array (the second argument).
  */
class FunctionReplicate : public IFunction
{
public:
    static constexpr auto name = "replicate";
    static FunctionPtr create(const Context & context);

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    bool useDefaultImplementationForNulls() const override { return false; }

    DataTypePtr getReturnTypeImpl(const DataTypes & arguments) const override;

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override;
};


/// Executes expression. Uses for lambda functions implementation. Can't be created from factory.
class FunctionExpression : public IFunctionBase
    , public IExecutableFunction
    , public std::enable_shared_from_this<FunctionExpression>
{
public:
    FunctionExpression(
        const ExpressionActionsPtr & expression_actions,
        const DataTypes & argument_types,
        const Names & argument_names,
        const DataTypePtr & return_type,
        const std::string & return_name)
        : expression_actions(expression_actions)
        , argument_types(argument_types)
        , argument_names(argument_names)
        , return_type(return_type)
        , return_name(return_name)
    {
    }

    String getName() const override { return "FunctionExpression"; }

    const DataTypes & getArgumentTypes() const override { return argument_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    bool useDefaultImplementationForNulls() const override { return false; }

    ExecutableFunctionPtr prepare(const Block &) const override
    {
        return std::const_pointer_cast<FunctionExpression>(shared_from_this());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        Block expr_block;
        for (size_t i = 0; i < arguments.size(); ++i)
        {
            const auto & argument = block.getByPosition(arguments[i]);
            /// Replace column name with value from argument_names.
            expr_block.insert({argument.column, argument.type, argument_names[i]});
        }

        expression_actions->execute(expr_block);

        block.getByPosition(result).column = expr_block.getByName(return_name).column;
    }

private:
    ExpressionActionsPtr expression_actions;
    DataTypes argument_types;
    Names argument_names;
    DataTypePtr return_type;
    std::string return_name;
};

/// Captures columns which are used by lambda function but not in argument list.
/// Returns ColumnFunction with captured columns.
/// For lambda(x, x + y) x is in lambda_arguments, y is in captured arguments, expression_actions is 'x + y'.
///  execute(y) returns ColumnFunction(FunctionExpression(x + y), y) with type Function(x) -> function_return_type.
class FunctionCapture : public IFunctionBase
    , public IExecutableFunction
    , public IFunctionBuilder
    , public std::enable_shared_from_this<FunctionCapture>
{
public:
    FunctionCapture(const ExpressionActionsPtr & expression_actions, const Names & captured, const NamesAndTypesList & lambda_arguments, const DataTypePtr & function_return_type, const std::string & expression_return_name)
        : expression_actions(expression_actions)
        , captured_names(captured)
        , lambda_arguments(lambda_arguments)
        , function_return_type(function_return_type)
        , expression_return_name(expression_return_name)
    {
        const auto & all_arguments = expression_actions->getRequiredColumnsWithTypes();

        std::unordered_map<std::string, DataTypePtr> arguments_map;
        for (const auto & arg : all_arguments)
            arguments_map[arg.name] = arg.type;

        auto collect = [&arguments_map](const Names & names) {
            DataTypes types;
            types.reserve(names.size());
            for (const auto & name : names)
            {
                auto it = arguments_map.find(name);
                if (it == arguments_map.end())
                    throw Exception("Lambda captured argument " + name + " not found in required columns.",
                                    ErrorCodes::LOGICAL_ERROR);

                types.push_back(it->second);
                arguments_map.erase(it);
            }

            return types;
        };

        captured_types = collect(captured_names);

        DataTypes argument_types;
        argument_types.reserve(lambda_arguments.size());
        for (const auto & lambda_argument : lambda_arguments)
            argument_types.push_back(lambda_argument.type);

        return_type = std::make_shared<DataTypeFunction>(argument_types, function_return_type);

        name = "Capture[" + toString(captured_types) + "](" + toString(argument_types) + ") -> "
            + function_return_type->getName();
    }

    String getName() const override { return name; }

    const DataTypes & getArgumentTypes() const override { return captured_types; }
    const DataTypePtr & getReturnType() const override { return return_type; }

    ExecutableFunctionPtr prepare(const Block &) const override
    {
        return std::const_pointer_cast<FunctionCapture>(shared_from_this());
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        ColumnsWithTypeAndName columns;
        columns.reserve(arguments.size());

        Names names;
        DataTypes types;

        names.reserve(captured_names.size() + lambda_arguments.size());
        names.insert(names.end(), captured_names.begin(), captured_names.end());

        types.reserve(captured_types.size() + lambda_arguments.size());
        types.insert(types.end(), captured_types.begin(), captured_types.end());

        for (const auto & lambda_argument : lambda_arguments)
        {
            names.push_back(lambda_argument.name);
            types.push_back(lambda_argument.type);
        }

        for (const auto & argument : arguments)
            columns.push_back(block.getByPosition(argument));

        auto function = std::make_shared<FunctionExpression>(expression_actions, types, names, function_return_type, expression_return_name);
        auto size = block.rows();
        block.getByPosition(result).column = ColumnFunction::create(size, std::move(function), columns);
    }

    size_t getNumberOfArguments() const override { return captured_types.size(); }

protected:
    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName &) const override { return return_type; }
    bool useDefaultImplementationForNulls() const override { return false; }
    FunctionBasePtr buildImpl(
        const ColumnsWithTypeAndName &,
        const DataTypePtr &,
        const TiDB::TiDBCollatorPtr &) const override
    {
        return std::const_pointer_cast<FunctionCapture>(shared_from_this());
    }

private:
    std::string toString(const DataTypes & data_types) const
    {
        std::string result;
        {
            WriteBufferFromString buffer(result);
            bool first = true;
            for (const auto & type : data_types)
            {
                if (!first)
                    buffer << ", ";

                first = false;
                buffer << type->getName();
            }
        }

        return result;
    }

    ExpressionActionsPtr expression_actions;
    DataTypes captured_types;
    Names captured_names;
    NamesAndTypesList lambda_arguments;
    DataTypePtr function_return_type;
    DataTypePtr return_type;
    std::string expression_return_name;
    std::string name;
};


template <bool negative, bool global, bool ignore_null>
struct FunctionInName;
template <>
struct FunctionInName<false, false, true>
{
    static constexpr auto name = "in";
};
template <>
struct FunctionInName<false, false, false>
{
    static constexpr auto name = "tidbIn";
};
template <>
struct FunctionInName<false, true, true>
{
    static constexpr auto name = "globalIn";
};
template <>
struct FunctionInName<true, false, true>
{
    static constexpr auto name = "notIn";
};
template <>
struct FunctionInName<true, false, false>
{
    static constexpr auto name = "tidbNotIn";
};
template <>
struct FunctionInName<true, true, true>
{
    static constexpr auto name = "globalNotIn";
};

static Field FIELD_INT32_1 = toField(Int32(1));

template <bool negative, bool global, bool ignore_null>
class FunctionIn : public IFunction
{
public:
    static constexpr auto name = FunctionInName<negative, global, ignore_null>::name;
    static FunctionPtr create(const Context &)
    {
        return std::make_shared<FunctionIn>();
    }

    String getName() const override
    {
        return name;
    }

    size_t getNumberOfArguments() const override
    {
        return 2;
    }

    DataTypePtr getReturnTypeImpl(const ColumnsWithTypeAndName & arguments) const override
    {
        if constexpr (ignore_null)
            return std::make_shared<DataTypeUInt8>();

        auto type = removeNullable(arguments[0].type);
        if (typeid_cast<const DataTypeTuple *>(type.get()))
            throw Exception("Illegal type (" + arguments[0].type->getName() + ") of 1 argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        if (!typeid_cast<const DataTypeSet *>(arguments[1].type.get()))
            throw Exception("Illegal type (" + arguments[1].type->getName() + ") of 2 argument of function " + getName(),
                            ErrorCodes::ILLEGAL_TYPE_OF_ARGUMENT);
        bool return_nullable = arguments[0].type->isNullable();
        ColumnPtr column_set_ptr = arguments[1].column;
        const auto * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
        return_nullable |= column_set->getData()->containsNullValue();

        if (return_nullable)
            return makeNullable(std::make_shared<DataTypeUInt8>());
        else
            return std::make_shared<DataTypeUInt8>();
    }

    bool useDefaultImplementationForNulls() const override
    {
        return false;
    }

    void executeImpl(Block & block, const ColumnNumbers & arguments, size_t result) const override
    {
        if (pass_through) {
            block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), FIELD_INT32_1);
            return;
        }
        const ColumnWithTypeAndName & left_arg = block.getByPosition(arguments[0]);
        if constexpr (!ignore_null)
        {
            if (left_arg.type->onlyNull())
            {
                block.getByPosition(result).column = block.getByPosition(result).type->createColumnConst(block.rows(), Null());
                return;
            }
        }
        /// Second argument must be ColumnSet.
        ColumnPtr column_set_ptr = block.getByPosition(arguments[1]).column;
        if (set_ptr)
            column_set_ptr = set_ptr;
        const auto * column_set = typeid_cast<const ColumnSet *>(&*column_set_ptr);
        if (!column_set)
            throw Exception("Second argument for function '" + getName() + "' must be Set; found " + column_set_ptr->getName(),
                            ErrorCodes::ILLEGAL_COLUMN);

        Block block_of_key_columns;

        /// First argument may be tuple or single column.
        const auto * tuple = typeid_cast<const ColumnTuple *>(left_arg.column.get());
        const ColumnConst * const_tuple = checkAndGetColumnConst<ColumnTuple>(left_arg.column.get());
        const auto * type_tuple = typeid_cast<const DataTypeTuple *>(left_arg.type.get());

        ColumnPtr materialized_tuple;
        if (const_tuple)
        {
            materialized_tuple = const_tuple->convertToFullColumn();
            tuple = typeid_cast<const ColumnTuple *>(materialized_tuple.get());
        }
        auto left_column_vector = left_arg.column;
        if (left_arg.column->isColumnConst())
        {
            left_column_vector = left_column_vector->convertToFullColumnIfConst();
        }

        if (tuple)
        {
            const Columns & tuple_columns = tuple->getColumns();
            const DataTypes & tuple_types = type_tuple->getElements();
            size_t tuple_size = tuple_columns.size();
            for (size_t i = 0; i < tuple_size; ++i)
                block_of_key_columns.insert({tuple_columns[i], tuple_types[i], ""});
        }
        else
        {
            if (left_arg.column->isColumnConst())
            {
                block_of_key_columns.insert({left_column_vector, left_arg.type, ""});
            }
            else
            {
                block_of_key_columns.insert(left_arg);
            }
        }

        if constexpr (ignore_null)
        {
            block.getByPosition(result).column = column_set->getData()->execute(block_of_key_columns, negative);
        }
        else
        {
            bool set_contains_null = column_set->getData()->containsNullValue();
            bool return_nullable = left_arg.type->isNullable() || set_contains_null;
            if (return_nullable)
            {
                auto nested_res = column_set->getData()->execute(block_of_key_columns, negative);
                if (left_column_vector->isColumnNullable())
                {
                    ColumnPtr result_null_map_column = dynamic_cast<const ColumnNullable &>(*left_column_vector).getNullMapColumnPtr();
                    if (set_contains_null)
                    {
                        MutableColumnPtr mutable_result_null_map_column = (*std::move(result_null_map_column)).mutate();
                        NullMap & result_null_map = dynamic_cast<ColumnUInt8 &>(*mutable_result_null_map_column).getData();
                        const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(nested_res.get());
                        const auto & data = uint8_column->getData();
                        for (size_t i = 0, size = result_null_map.size(); i < size; i++)
                        {
                            if (data[i] == negative)
                                result_null_map[i] = 1;
                        }
                        result_null_map_column = std::move(mutable_result_null_map_column);
                    }
                    block.getByPosition(result).column = ColumnNullable::create(nested_res, result_null_map_column);
                }
                else
                {
                    auto col_null_map = ColumnUInt8::create();
                    ColumnUInt8::Container & vec_null_map = col_null_map->getData();
                    vec_null_map.assign(block.rows(), static_cast<UInt8>(0));
                    const auto * uint8_column = checkAndGetColumn<ColumnUInt8>(nested_res.get());
                    const auto & data = uint8_column->getData();
                    for (size_t i = 0, size = vec_null_map.size(); i < size; i++)
                    {
                        if (data[i] == negative)
                            vec_null_map[i] = 1;
                    }
                    block.getByPosition(result).column = ColumnNullable::create(nested_res, std::move(col_null_map));
                }
            }
            else
            {
                block.getByPosition(result).column = column_set->getData()->execute(block_of_key_columns, negative);
            }
        }
    }

    void setPassThrough() {
        pass_through = true;
    }
    void setSetPtr(ColumnPtr new_set_ptr) {
        set_ptr = new_set_ptr;
    }
private:
    ColumnPtr set_ptr = nullptr;
    bool pass_through = false;
};


} // namespace DB
