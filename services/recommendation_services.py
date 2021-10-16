from services import db_services


async def get_item(item_id: int, connection_pool):
    sql_query_string = "SELECT * FROM testTable0 where keys = (%s);"
    sql_query_params = (item_id,)
    result = await db_services.execute_query(
        sql_query_string, sql_query_params, connection_pool, fetch="one"
    )

    return result
