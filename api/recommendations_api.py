import fastapi
from fastapi import Depends, Request

from services import recommendation_services

router = fastapi.APIRouter()


@router.get("/get_recommendations/{user_id}")
async def get_test_item(user_id: int, req: Request):
    recommendations = await recommendation_services.get_recommendations(user_id, req.app.state.pool)
    history = await recommendation_services.get_history(user_id, req.app.state.pool)

    response = {"recommendations": recommendations, "history": history}

    return response


@router.get("/get_recommendations_mock/{user_id}")
async def get_test_item(user_id: int, req: Request):
    user_id = int(user_id)
    data =  {1:
                {"recommendations":
                    [
                        {"id": 789,
                        "title": "Красная шапочка",
                        "author": "Перро",
                        "rank": 1},
                        {"id": 101112,
                        "title": "Сказки",
                        "author": "народ",
                        "rank": 2},
                        {"id": 111,
                        "title": "ааа",
                        "author": "тт",
                        "rank": 4},
                        {"id": 222,
                        "title": "ббб",
                        "author": "ии",
                        "rank": 5},
                        {"id": 333,
                        "title": "000",
                        "author": "ььь",
                        "rank": 3},
                    ],
                "history":
                    [
                        {"id": 1,
                        "title": "белая шапочка",
                        "author": "мерро"},
                        {"id": 2,
                        "title": "1",
                        "author": "11"},
                        {"id": 4,
                        "title": "4",
                        "author": "44"},
                        {"id": 5,
                        "title": "5",
                        "author": "55"},
                        {"id": 3,
                        "title": "3",
                        "author": "3"},
                    ],
                },
            2:
                {"recommendations":
                    [
                        {"id": 11,
                        "title": "Красная шапочка",
                        "author": "Перро",
                        "rank": 1},
                        {"id": 12,
                        "title": "Сказки",
                        "author": "народ",
                        "rank": 2},
                        {"id": 13,
                        "title": "ааа",
                        "author": "тт",
                        "rank": 4},
                        {"id": 14,
                        "title": "ббб",
                        "author": "ии",
                        "rank": 5},
                        {"id": 15,
                        "title": "000",
                        "author": "ььь",
                        "rank": 3},
                    ],
                "history":
                    [
                        {"id": 10,
                        "title": "белая шапочка",
                        "author": "мерро"},
                        {"id": 20,
                        "title": "1",
                        "author": "11"},
                        {"id": 40,
                        "title": "4",
                        "author": "44"},
                        {"id": 50,
                        "title": "5",
                        "author": "55"},
                        {"id": 30,
                        "title": "3",
                        "author": "3"},
                    ],
                },
            0:  {"recommendations":
                    [
                        {"id": 9,
                        "title": "Красная шапочка",
                        "author": "Перро",
                        "rank": 1},
                        {"id": 101112,
                        "title": "Сказки",
                        "author": "народ",
                        "rank": 2},
                        {"id": 999,
                        "title": "ааа",
                        "author": "тт",
                        "rank": 4},
                        {"id": 9999,
                        "title": "ббб",
                        "author": "ии",
                        "rank": 5},
                        {"id": 99999,
                        "title": "000",
                        "author": "ььь",
                        "rank": 3},
                    ],
                "history":
                    [
                        {"id": 100,
                        "title": "белая шапочка",
                        "author": "мерро"},
                        {"id": 200,
                        "title": "1",
                        "author": "11"},
                        {"id": 400,
                        "title": "4",
                        "author": "44"},
                        {"id": 500,
                        "title": "5",
                        "author": "55"},
                        {"id": 300,
                        "title": "3",
                        "author": "3"},
                    ],
                },
                    }
        
    response = data.get(user_id, data[0])

    return response

