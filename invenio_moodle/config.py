# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Graz University of Technology.
#
# invenio-moodle is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Default configuration for invenio-moodle."""

from celery.schedules import crontab
from flask_babelex import gettext as _
from invenio_rdm_records.services.pids import providers

MOODLE_CELERY_BEAT_SCHEDULE = {
    "moodle": {
        "task": "invenio_moodle.tasks.fetch_moodle",
        "schedule": crontab(minute=30, hour=2, day_of_month=10, month_of_year="2,7"),
    }
}

MOODLE_PERSISTENT_IDENTIFIER_PROVIDERS = [
    providers.ExternalPIDProvider("moodle", "moodle", label=_("MOODLE ID"))
]

MOODLE_PERSISTENT_IDENTIFIERS = {
    "moodle": {
        "providers": ["moodle"],
        "required": False,
        "label": _("MOODLE"),
    }
}

MOODLE_FETCH_URL = "https://tc.tugraz.at/main/local/oer/public_metadata.php"

MOODLE_ERROR_MAIL_SENDER = ""
MOODLE_ERROR_MAIL_RECIPIENTS = []
