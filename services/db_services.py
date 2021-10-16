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

    return result
