from services import db_services


async def get_recommendations(user_id: int, connection_pool):
    sql_query_string = "SELECT item_id, title, author, rank FROM recommendations where user_id = (%s);"
    sql_query_params = (user_id,)
    query_output = await db_services.execute_query(
        sql_query_string, sql_query_params, connection_pool, fetch="all"
    )
    result = [{"id": row[0], "title": row[1], "author": row[2], "rank": row[3]}
        for row in query_output]

    return result


async def get_history(user_id: int, connection_pool):
    sql_query_string = "SELECT item_id, title, author FROM history where user_id = (%s);"
    sql_query_params = (user_id,)
    query_output = await db_services.execute_query(
        sql_query_string, sql_query_params, connection_pool, fetch="all"
    )
    result = [{"id": row[0], "title": row[1], "author": row[2], "rank": row[3]}
        for row in query_output]

    return result
