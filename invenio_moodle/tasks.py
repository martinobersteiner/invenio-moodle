# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Graz University of Technology.
#
# invenio-moodle is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Celery tasks for `invenio-moodle`."""

import traceback

import requests
from celery import shared_task
from flask import current_app
from flask_mail import Message

from .utils import insert_moodle_into_db


@shared_task(ignore_result=True)
def fetch_moodle():
    """Fetch data from moodle and enter it into database."""
    try:
        response = requests.get(current_app.config["MOODLE_FETCH_URL"])
        response.raise_for_status()

        moodle_data = response.json()

        insert_moodle_into_db(moodle_data)

    except Exception:  # pylint: disable=broad-except
        msg = Message(
            "Something went wrong when fetching data from moodle",
            sender=current_app.config["MOODLE_ERROR_MAIL_SENDER"],
            recipients=current_app.config["MOODLE_ERROR_MAIL_RECIPIENTS"],
            body=traceback.format_exc(),
        )
        # TODO: is mail configured correctly on production server?
        current_app.extensions["mail"].send(msg)
