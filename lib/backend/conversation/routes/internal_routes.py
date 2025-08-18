# Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
# SPDX-License-Identifier: Apache-2.0
from typing import Dict

from aws_lambda_powertools import Logger, Tracer
from aws_lambda_powertools.event_handler.api_gateway import Router
from conversation_store import get_chat_history_store

from .types import CreateInternalChatMessagesInput

tracer = Tracer()
router = Router()
logger = Logger()


@router.get("/internal/user/<user_id>/chat/<chat_id>")
@tracer.capture_method(capture_response=False)
def list_internal_chat_messages(user_id: str, chat_id: str) -> Dict:
    limit = int(router.current_event.query_string_parameters.get("limit", 20))  # type: ignore
    chat_history_store = get_chat_history_store()
    
    ####
    include_sources = router.current_event.query_string_parameters.get("include_sources", "false").lower() == "true"

    order = router.current_event.query_string_parameters.get("order", "desc")
    ascending = (order == "asc")

    wantTitle = router.current_event.query_string_parameters.get("wantTitle","no")
    needTitle = (wantTitle == "yes")
    title = None
    if needTitle:
        title = chat_history_store.get_chat_title(user_id=user_id,chat_id=chat_id)

    newTitle = router.current_event.query_string_parameters.get("updateTitle",None)
    if newTitle is not None:
        chat_history_store.update_chat(user_id=user_id, chat_id=chat_id, chat_title=newTitle)
    ####

    messages, _ = chat_history_store.list_chat_messages(
        user_id=user_id,
        chat_id=chat_id,
        limit=limit,
        ascending=ascending,
    )

    ####
    response_messages = []
    for message in messages:
        message_dict = message.dict()
        if include_sources and message.messageType == "ai":
            sources = chat_history_store.list_chat_message_sources(
                user_id=user_id, 
                message_id=message.messageId
            )
            if sources:
                message_dict["sources"] = [source.dict() for source in sources]
        response_messages.append(message_dict)
    ####

    if title is None:
        return {"data": {"messages": response_messages}}
    else:
        return {"data": {"messages": response_messages, "title": title}}


@router.put("/internal/user/<user_id>/chat/<chat_id>/message")
@tracer.capture_method(capture_response=False)
def add_internal_chat_message(user_id: str, chat_id: str) -> Dict:
    request = CreateInternalChatMessagesInput(**router.current_event.body)  # type: ignore
    chat_history_store = get_chat_history_store()

    message_type = "ai" if request.role == "assistant" else "human"

    tokens = router.current_event.body.get("tokens", 0)
    model_id = router.current_event.body.get("model_id")
    
    
    chat_message = chat_history_store.create_chat_message(
        user_id=user_id, chat_id=chat_id, content=request.content, message_type=message_type, tokens=tokens, sources=request.sources
    )

    chat_history_store.update_cost(user_id=user_id, chat_id=chat_id, tokens=tokens, model_id=model_id, message_type=message_type)

    return {
        "data": {
            "message": chat_message.dict(),
        }
    }
