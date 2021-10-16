from services import db_services

RECOMMENDATIONS_TABLE = "recommendations"
RECOMMENDATIONS_FIELDS = ["item_id", "title", "author", "ranking"]
HISTORY_TABLE = "history"
HISTORY_FIELDS = ["item_id", "title", "author"]
USER_FIELD = "user_id"
DEFAULT_USER_ID = 0


async def get_recommendations(user_id: int, connection_pool):
    query_output = await db_services.select_with_filter(
        connection_pool,
        RECOMMENDATIONS_TABLE,
        RECOMMENDATIONS_FIELDS,
        USER_FIELD,
        user_id,
        DEFAULT_USER_ID
    )
    result = [{"id": row[0], "title": row[1], "author": row[2], "rank": row[3]}
        for row in query_output]

    return result


async def get_history(user_id: int, connection_pool):
    query_output = await db_services.select_with_filter(
        connection_pool,
        HISTORY_TABLE,
        HISTORY_FIELDS,
        USER_FIELD,
        user_id,
        DEFAULT_USER_ID
    )
    result = [{"id": row[0], "title": row[1], "author": row[2]}
        for row in query_output]

    return result
