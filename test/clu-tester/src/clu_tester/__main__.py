#!/usr/bin/env python
# -*- coding: utf-8 -*-
#
# @Author: José Sánchez-Gallego (gallegoj@uw.edu)
# @Date: 2025-11-22
# @Filename: __main__.py
# @License: BSD 3-clause (http://www.opensource.org/licenses/BSD-3-Clause)

from __future__ import annotations

import os
import warnings

import click
from click_default_group import DefaultGroup
from clu.exceptions import CluWarning
from sdsstools.daemonizer import DaemonGroup, cli_coro

from clu_tester.actor import CLUTesterActor


@click.group(
    cls=DefaultGroup,
    default="actor",
    default_if_no_args=True,
    invoke_without_command=True,
)
@click.pass_context
def clu_tester(ctx: click.Context):
    """clu-tester command line interface."""

    pass


@clu_tester.group(cls=DaemonGroup, prog="clu-tester-actor", workdir=os.getcwd())
@click.pass_context
@cli_coro()
async def actor(ctx: click.Context):
    """Runs the actor."""

    clu_tester_obj = CLUTesterActor(
        "clu-tester",
        "0.0.0.0",
        9000,
        log=False,
        version="0.1.0",
    )

    with warnings.catch_warnings():
        warnings.simplefilter("ignore", category=CluWarning)
        await clu_tester_obj.start()

    await clu_tester_obj.run_forever()


if __name__ == "__main__":
    clu_tester()
