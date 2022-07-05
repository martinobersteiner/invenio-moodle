# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Graz University of Technology.
#
# invenio-moodle is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Utilities for inserting moodle-data into invenio-style database."""

from __future__ import annotations

import copy
import html
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import partial

from invenio_access.permissions import system_identity
from invenio_pidstore.errors import PIDDoesNotExistError
from invenio_pidstore.models import PersistentIdentifier
from invenio_records_lom.proxies import current_records_lom
from invenio_records_lom.utils import LOMMetadata
from invenio_records_resources.services.uow import UnitOfWork

from .schemas import MoodleSchema


@dataclass(frozen=True)
class Key(ABC):
    """Common ancestor to all SomethingKey classes."""

    @abstractmethod
    def to_string_key(self):
        """Convert `self` to unique string representation."""


@dataclass(frozen=True)
class CourseKey(Key):
    """Key for courses as to disambiguate it from keys for units and files."""

    courseid: str

    @classmethod
    def from_json(cls, moodle_course_json):
        """Create `cls` via info from moodle-json."""
        courseid = moodle_course_json["courseid"]
        return cls(courseid=courseid)

    def to_string_key(self):
        """Get string-representation."""
        return f"CourseKey(courseid={self.courseid})"


@dataclass(frozen=True)
class UnitKey(Key):
    """Key for units as to disambiguate it from keys for courses and files."""

    courseid: str
    year: str
    semester: str

    @classmethod
    def from_json(cls, moodle_file_json, moodle_course_json):
        """Create `cls` via info from moodle-json."""
        courseid = moodle_course_json["courseid"]
        year = moodle_file_json["year"]
        semester = moodle_file_json["semester"]
        return cls(courseid=courseid, year=year, semester=semester)

    def to_string_key(self):
        """Get string-representation."""
        return f"UnitKey(courseid={self.courseid}, year={self.year}, semester={self.semester})"


@dataclass
class TaskInfo:
    """Stores data."""

    pid: str
    previous_json: dict
    json: dict
    moodle_file_json: dict = None
    moodle_course_json: dict = None


def fetch_else_create(database_key, resource_type):
    """Fetch moodle-result corresponding to `database_key`, create database-entry if none exists."""
    service = current_records_lom.records_service
    create = partial(service.create, identity=system_identity)
    read = partial(service.read, identity=system_identity)

    try:
        moodle_pid = PersistentIdentifier.get(pid_type="moodle", pid_value=database_key)
    except PIDDoesNotExistError:
        # create draft with empty metadata
        pids_dict = {"moodle": {"provider": "moodle", "identifier": database_key}}
        metadata = LOMMetadata.create(resource_type=resource_type, pids=pids_dict)
        metadata.append_identifier(database_key, catalog="moodle")
        draft_item = create(data=metadata.json)

        pid: str = draft_item.id
        previous_json = None
        json_ = draft_item.to_dict()
    else:
        # get lomid corresponding to moodle_pid
        lomid_pid = PersistentIdentifier.get_by_object(
            pid_type="lomid",
            object_type=moodle_pid.object_type,
            object_uuid=moodle_pid.object_uuid,
        )

        pid: str = lomid_pid.pid_value
        previous_json = read(id_=pid).to_dict()
        json_ = copy.deepcopy(previous_json)

    return TaskInfo(pid=pid, previous_json=previous_json, json=json_)


def link_up(whole: TaskInfo, part: TaskInfo):
    """If unlinked, link jsons within `whole`, `part`."""
    whole_metadata = LOMMetadata(whole.json)
    part_metadata = LOMMetadata(part.json)

    whole_metadata.append_relation(part.pid, kind="haspart")
    part_metadata.append_relation(whole.pid, kind="ispartof")

    whole.json = whole_metadata.json
    part.json = part_metadata.json


def update_course_metadata(course_item: TaskInfo):
    """Convert moodle-style file-json to LOM json."""
    metadata = LOMMetadata(course_item.json or {}, overwritable=True)
    file_json = course_item.moodle_file_json
    course_json = course_item.moodle_course_json

    # convert courseid
    courseid = course_json["courseid"]
    metadata.append_identifier(courseid, "moodle-id")

    # convert coursename
    coursename = course_json["coursename"]
    # TODO: C: is the following language always correct?
    # e.g. I had courses taught in english one year and german the next
    #   their description was english one year and german the next
    #   their official title stayed german throughout though...
    language = course_json["courselanguage"]
    metadata.set_title(coursename, language_code=language)

    # convert context
    context = file_json["context"]
    metadata.append_context(context)

    # TODO: C: convert structure:
    # - contains values like 'Seminar (SE)', 'Vorlesung (VO)'
    # - values are of a controlled vocabulary
    # structure = course_json["structure"]

    course_item.json = metadata.json


def update_unit_metadata(unit_item: TaskInfo):
    """Convert moodle-style file-json to LOM json."""
    metadata = LOMMetadata(unit_item.json or {}, overwritable=True)
    file_json = unit_item.moodle_file_json
    course_json = unit_item.moodle_course_json

    # multi-use input-data
    language = course_json["courselanguage"]

    # convert title
    coursename = course_json["coursename"]
    year = file_json["year"]
    semester = file_json["semester"]
    title = f"{coursename} ({semester} {year})"
    metadata.set_title(title, language_code=language)

    # convert language
    metadata.append_language(language)

    # convert description
    description = html.unescape(course_json["description"])
    metadata.append_description(description, language_code=language)

    # convert lecturers
    for lecturer in course_json["lecturer"].split(","):
        lecturer = lecturer.strip()
        metadata.append_contribute(lecturer, role="Author")

    # TODO: C: convert organisation
    # - e.g. "Institut für Elektronik"
    # - not sure where to put this...
    # organisation = course_json['organisation']

    # TODO: C: convert objective
    # - this is some longwinded explanation of what students are supposed to learn in this course
    # - `lom.general.description` is taken by course_json['description'] already...
    # - not sure where to put this...
    # objective = course_json['objective']

    # TODO: C: add year, semester to some lom field?

    # TODO: C: `lom.lifecycle.datetime`
    # - should it be set for units here?
    # - if so, which value?

    unit_item.json = metadata.json


def insert_moodle_into_db(moodle_data: dict):
    """Insert data encoded in `moodle-data` into invenio-database.

    :param dict moodle_data: The data to be inserted into database,
        whose format matches `MoodleSchema`
    """
    # TODO: refactor: split up this function
    # TODO: link with previous course

    # validate input
    moodle_data = MoodleSchema().load(moodle_data)

    # initialize
    tasks: dict[Key, TaskInfo] = {}  # keeps track of one task per course/unit/file
    links: set[tuple[Key, Key]] = set()  # {(course_key, "haspart", unit_key), ...}
    moodle_file_jsons = [
        file_json
        for moodlecourse in moodle_data["moodlecourses"]
        for file_json in moodlecourse["files"]
    ]

    # prepare: gather necessary information, create records if no previous versions exist
    for moodle_file_json in moodle_file_jsons:
        # TODO: add fetching of files here (in a later PR)
        for moodle_course_json in moodle_file_json["courses"]:
            unit_key = UnitKey.from_json(moodle_file_json, moodle_course_json)
            course_key = CourseKey.from_json(moodle_course_json)

            if unit_key not in tasks:
                unit_item = fetch_else_create(
                    unit_key.to_string_key(),
                    resource_type="unit",
                )
                unit_item.moodle_file_json = moodle_file_json
                unit_item.moodle_course_json = moodle_course_json
                tasks[unit_key] = unit_item

            if course_key not in tasks:
                course_item = fetch_else_create(
                    course_key.to_string_key(),
                    resource_type="course",
                )
                course_item.moodle_file_json = moodle_file_json
                course_item.moodle_course_json = moodle_course_json
                tasks[course_key] = course_item

            links.add((course_key, unit_key))

    # link records
    for whole_key, part_key in links:
        link_up(tasks[whole_key], tasks[part_key])

    # update lom-jsons with info from moodle
    for key, item in tasks.items():
        if isinstance(key, UnitKey):
            update_unit_metadata(item)
        elif isinstance(key, CourseKey):
            update_course_metadata(item)
        else:
            raise TypeError("Cannot handle key of type {type(key)}.")

    # update drafts
    service = current_records_lom.records_service
    edit = partial(service.edit, identity=system_identity)
    update_draft = partial(service.update_draft, identity=system_identity)
    for item in tasks.values():
        if item.previous_json != item.json:
            # json got updated, now update database with new json
            edit(id_=item.pid)  # ensure a draft exists
            update_draft(id_=item.pid, data=item.json)

    # publish created drafts
    # uow rolls back all `.publish`s if one fails as to prevent an inconsistent database-state
    publish = partial(service.publish, identity=system_identity)
    with UnitOfWork() as uow:
        for item in tasks.values():
            if item.previous_json != item.json:
                publish(id_=item.pid, uow=uow)

        uow.commit()
