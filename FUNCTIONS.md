# Named Parameters in DuckDB Extension Functions

## Table Functions

Table functions natively support named parameters via `SimpleNamedParameterFunction` base class.

```cpp
// Bind function receives named params in input.named_parameters
static unique_ptr<FunctionData> MyTableBind(ClientContext &context, TableFunctionBindInput &input,
                                            vector<LogicalType> &return_types, vector<string> &names) {
    auto result = make_uniq<MyBindData>();
    result->url = input.inputs[0].GetValue<string>();  // positional arg

    // Named parameters
    auto headers_entry = input.named_parameters.find("headers");
    if (headers_entry != input.named_parameters.end()) {
        result->headers = headers_entry->second;
    }

    return result;
}

// Registration
TableFunction my_func("my_func", {LogicalType::VARCHAR}, MyTableFunction, MyTableBind, MyTableInit);
my_func.named_parameters["headers"] = LogicalType::ANY;
my_func.named_parameters["params"] = LogicalType::ANY;
loader.RegisterFunction(my_func);
```

SQL usage:

```sql
SELECT * FROM my_func('https://example.com', headers := {'Accept': 'application/json'});
```

## Scalar Functions

Scalar functions don't natively support named parameters - arguments are passed positionally. However, you can detect `:=` syntax via `Expression::alias` in a bind function.

```cpp
// Bind function reorders arguments based on aliases
static unique_ptr<FunctionData> MyScalarBind(ClientContext &context, ScalarFunction &bound_function,
                                             vector<unique_ptr<Expression>> &arguments) {
    if (arguments.size() == 3) {
        idx_t headers_idx = 1;  // expected position
        idx_t params_idx = 2;   // expected position

        // Detect aliases from := syntax
        for (idx_t i = 1; i < arguments.size(); i++) {
            if (StringUtil::CIEquals(arguments[i]->alias, "headers")) {
                headers_idx = i;
            } else if (StringUtil::CIEquals(arguments[i]->alias, "params")) {
                params_idx = i;
            }
        }

        // Swap if out of order
        if (headers_idx == 2 && params_idx == 1) {
            std::swap(arguments[1], arguments[2]);
        }
    }
    return nullptr;
}

// Registration - pass bind function as 4th argument
ScalarFunction my_scalar({LogicalType::VARCHAR, LogicalType::ANY, LogicalType::ANY},
                         return_type, MyScalarFunc, MyScalarBind);
loader.RegisterFunction(my_scalar);
```

SQL usage:

```sql
-- Both work identically:
SELECT my_func('url', headers := {...}, params := {...});
SELECT my_func('url', params := {...}, headers := {...});
```

## Key Differences

| Feature | TableFunction | ScalarFunction |
|---------|---------------|----------------|
| Native named params | Yes (`named_parameters` map) | No |
| Access method | `input.named_parameters` | `arguments[i]->alias` in bind |
| Reordering needed | No | Yes (manual swap) |
