#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2025-11-22
# @Filename: actor.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import click
from clu.command import Command
from clu.legacy.actor import LegacyActor
from clu.parsers.click import command_parser


class CLUTesterActor(LegacyActor):
    """A minimal legacy actor for testing ``clu-middleware-tron``."""

    name = "clu-tester"

    def __init__(self, *args, **kwargs):
        additional_properties = kwargs.pop("additional_properties", True)
        super().__init__(*args, additional_properties=additional_properties, **kwargs)


@command_parser.command()
@click.argument("reply-type", type=str, required=False)
@click.option("--broadcast", is_flag=True, help="Broadcast the reply to all clients.")
async def emit_reply(
    command: Command[CLUTesterActor],
    reply_type: str | None = None,
    broadcast: bool = False,
):
    """Emits a reply message for testing."""

    message = {
        "key1": "value1",
        "key2": 42,
        "key3": 3.14,
        "key4": True,
        "key5": None,
        "key6": [1, False, "three", "a string; with; semicolons"],
        "key7": "A string with spaces",
        "key8": "A string; with; semicolons",
    }

    if reply_type is not None:
        if reply_type == "default":
            pass
        elif reply_type == "empty":
            message = {}
        elif reply_type == "nested":
            message = {"level1": {"level2": {"level3": "deep value"}}}
        else:
            return command.fail(f"Unknown reply type '{reply_type}'.")

    return command.finish(message, broadcast=broadcast)
