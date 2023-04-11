# pylint: disable=broad-except, attribute-defined-outside-init
from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Callable, Dict, List, Type, Union

from allocation.domain import commands, events

if TYPE_CHECKING:
    from . import unit_of_work

logger = logging.getLogger(__name__)

Message = Union[commands.Command, events.Event]


class MessageBus:
    def __init__(
        self,
        uow: unit_of_work.AbstractUnitOfWork,
        event_handlers: Dict[Type[events.Event], List[Callable]],
        command_handlers: Dict[Type[commands.Command], Callable],
    ):
        self.uow = uow
        self.event_handlers = event_handlers
        self.command_handlers = command_handlers

    async def handle(self, message: Message):
        self.queue = [message]
        while self.queue:
            message = self.queue.pop(0)
            if isinstance(message, events.Event):
                await self.handle_event(message)
            elif isinstance(message, commands.Command):
                await self.handle_command(message)
            else:
                raise Exception(f"{message} was not an Event or Command")

    async def handle_event(self, event: events.Event):
        for handler in self.event_handlers[type(event)]:
            try:
                logger.debug("handling event %s with handler %s", event, handler)
                await handler(event)
                self.queue.extend(self.uow.collect_new_events())
            except Exception:
                logger.exception("Exception handling event %s", event)
                continue

    async def handle_command(self, command: commands.Command):
        logger.debug("handling command %s", command)
        try:
            handler = self.command_handlers[type(command)]
            await handler(command)
            self.queue.extend(self.uow.collect_new_events())
        except Exception:
            logger.exception("Exception handling command %s", command)
            raise
