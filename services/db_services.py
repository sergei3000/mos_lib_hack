from typing import Any, List, Optional, Tuple, Union


async def execute_query(
    query_string: str, params: tuple, connection_pool, fetch: Optional[str] = None
) -> Union[Tuple[Any], List[Tuple[Any]], str]:
    """Generic execution of a query by the package `aiomysql`.

    Args:
        query_string (str): sql string in aiopg format
        params (tuple): parameters of the query string
        connection_pool ([type]): the main application's connection pool
        fetch (Optional[str], optional): "one" or "all" or None. Defaults to None.

    Returns:
        Result of the query or string "success".
    """
    assert fetch in ("one", "all", None)
    async with connection_pool.acquire() as conn:
        async with conn.cursor() as cur:
            await cur.execute(query_string, params)
            if fetch == "one":
                result = await cur.fetchone()
            elif fetch == "all":
                result = await cur.fetchall()
            else:
                result = "success"
        await conn.commit()

    return result


async def select_with_filter(connection_pool,
    table_name: str,
    selected_fields: List[str],
    filter_field: str,
    filter_value: Any,
    default_filter_value: Optional[Any] = None) -> Tuple[Any]:
    """Extract data from database table with filter

    Args:
        table_name (str): Table to extract data from
        selected_fields (List[str]): List of fields to select
        filter_field (str): Field to filter by
        filter_value (Any): Value in the filter_field
        default_filter_value (Optional[Any], optional): Default filter value. Defaults to None.

    Returns:
        Tuple[Any]: [description]
    """
    fields_string = ", ".join(selected_fields)
    sql_query_string = f"SELECT {fields_string} FROM {table_name} where {filter_field} = (%s);"
    sql_query_params = (filter_value,)

    result = await execute_query(
        sql_query_string, sql_query_params, connection_pool, fetch="all"
    )

    if not result:
        sql_query_params = (default_filter_value,)
        result = await execute_query(
            sql_query_string, sql_query_params, connection_pool, fetch="all"
        )
    
    return result
