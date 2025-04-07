from typing import List, Dict
import orjson
from django.http import HttpRequest, HttpResponse, HttpResponseNotAllowed, HttpResponseRedirect
from django.conf import settings
from django.db import connection, migrations, transaction
from django.db.backends.base.schema import BaseDatabaseSchemaEditor
from django.db.migrations.state import StateApps
from django.db.models import F, Func, JSONField, TextField, Value
from django.db.models.functions import Cast
from django.utils.timezone import now as timezone_now
from psycopg2.sql import SQL, Literal
from dataclasses import dataclass
from zerver.lib.typed_endpoint import typed_endpoint
from zerver.models import UserProfile
from zerver.lib.response import json_success

@dataclass
class TopicDataclass:
    name: str
    followed: bool

def _get_recipient_id_from_stream(stream_id: int) -> int:
    query = SQL(
        """
        SELECT recipient_id FROM zerver_stream
        WHERE id = {stream_id};
        """
    ).format(stream_id=Literal(stream_id))

    with connection.cursor() as cursor:
        cursor.execute(query)
        result = cursor.fetchone()
        assert result is not None
        return result[0]

def _process_topics(topics: Dict[str, TopicDataclass], user_id: int) -> Dict[str, TopicDataclass]:
    query = SQL(
        """
        SELECT topic_name, visibility_policy, user_profile_id FROM zerver_usertopic
        WHERE user_profile_id = {user_id};
        """
    ).format(user_id=Literal(user_id))

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        for topic in rows:
            FOLLOWED_POLICY: int = 3
            if topic[0] in topics.keys() and topic[1] == FOLLOWED_POLICY:
                topics[topic[0]].followed = True
    
    return topics

def get_topics_from_stream(stream_id: int, user_id: int) -> Dict[str, TopicDataclass]:
    recipient_id = _get_recipient_id_from_stream(stream_id)
    
    query = SQL(
        """
        SELECT subject, recipient_id FROM zerver_message
        WHERE recipient_id = {recipient_id}; 
        """
    ).format(recipient_id=Literal(recipient_id))

    result: Dict[str, TopicDataclass] = {}

    with connection.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        for topic in rows:
            topic_object = TopicDataclass(name=topic[0], followed=False)
            result[topic_object.name] = topic_object

    result = _process_topics(result, user_id)
    return result

@typed_endpoint
def get_topic_details(
    request: HttpRequest,
    user_profile: UserProfile,
    *,
    stream_id: int = -1,
    user_id: int = -1
) -> HttpResponse:
    resultDict: Dict[str, TopicDataclass] = get_topics_from_stream(stream_id, user_id)
    resultArray = []
    for key, value in resultDict.items():
        resultArray.append({ "name": value.name, "followed": value.followed })
    response = json_success(request, data={"data": resultArray})
    return response