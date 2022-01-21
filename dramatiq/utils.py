# This file is a part of Dramatiq.
#
# Copyright (C) 2019 CLEARTYPE SRL <bogdan@cleartype.io>
#
# Dramatiq is free software; you can redistribute it and/or modify it
# under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or (at
# your option) any later version.
#
# Dramatiq is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
# FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser General Public
# License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.
import os
import sys
import uuid
from typing import Dict, List

from loguru import logger

from .broker import get_broker
from .message import Message

DRAMATIQ_TOPIC_CREATION = (
    True if os.getenv("DRAMATIQ_TOPIC_CREATION") in ["true", "True"] else False
)


def get_logger(request_id=None):
    request_id = request_id if request_id else uuid.uuid4()
    logger.add(sys.stdout, format="{extra[request_id]} {message}")
    current_logger = logger.bind(request_id=request_id)
    return current_logger


def send_message(contract: Dict, args: List = None, kwargs: Dict = None) -> Message:
    """
    Sends message in broker

    Parameters
    ----------
    contract : dict
        Dramatiq contract for message in following format
    args : list
        Argument's list to be passed in message
    kwargs : list
        Keyword argument's to be passed

    Returns
    -------
        Message
    """
    queue_name = contract.get("queue_name")
    broker = get_broker()
    if DRAMATIQ_TOPIC_CREATION:
        broker.declare_queue(queue_name)
    message = Message(
        **contract,
        args=args or (),
        kwargs=kwargs or {},
    )
    broker.enqueue(message)
    return message
