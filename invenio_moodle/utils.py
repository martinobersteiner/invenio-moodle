# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Graz University of Technology.
#
# invenio-moodle is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Utilities for inserting moodle-data into invenio-style database."""

from __future__ import annotations

import copy
import hashlib
import html
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime
from functools import partial
from pathlib import Path
from tempfile import TemporaryDirectory

import requests
from invenio_access.permissions import system_identity
from invenio_pidstore.errors import PIDDoesNotExistError
from invenio_pidstore.models import PersistentIdentifier
from invenio_records_lom.proxies import current_records_lom
from invenio_records_lom.utils import LOMMetadata
from invenio_records_resources.services.uow import UnitOfWork

from .schemas import MoodleSchema


@dataclass(frozen=True)
class FileCacheInfo:
    """Holds a file-path and the file's md5-hash."""

    hash_md5: str
    path: Path


@dataclass(frozen=True)
class Key(ABC):
    """Common ancestor to all Key classes."""

    @abstractmethod
    def to_string_key(self):
        """Convert `self` to unique string representation."""


@dataclass(frozen=True)
class FileKey(Key):
    """Key for files as to disambiguate it from keys for units and courses."""

    hash_md5: str
    url: str

    def to_string_key(self):
        """Get string-representation."""
        # purposefully doesn't depend on `url`, as that might change
        return f"FileKey(hash_md5={self.hash_md5})"


@dataclass(frozen=True)
class UnitKey(Key):
    """Key for units as to disambiguate it from keys for files and courses."""

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


@dataclass(frozen=True)
class CourseKey(Key):
    """Key for courses as to disambiguate it from keys for files and units."""

    courseid: str

    @classmethod
    def from_json(cls, moodle_course_json):
        """Create `cls` via info from moodle-json."""
        courseid = moodle_course_json["courseid"]
        return cls(courseid=courseid)

    def to_string_key(self):
        """Get string-representation."""
        return f"CourseKey(courseid={self.courseid})"


@dataclass
class TaskLog:
    """Stores data."""

    pid: str
    previous_json: dict
    json: dict
    moodle_file_json: dict = None
    moodle_course_json: dict = None


def cache_files(
    directory: Path,
    provided_filepaths_by_url: dict[str, Path],
    urls: list[str],
) -> dict[str, FileCacheInfo]:
    """Creates a file-cache, downloading unprovided files into `directory` and hashing all files.

    :param Path directory: The directory to download unprovided files into.
    :param dict[str, Path] provided_filepaths_by_url: A dictionary that maps some urls to filepaths.
        When a url is in `provided_filepaths_by_url`,
        the file on the corresponding filepath is cached.
        Otherwise the file is downloaded from file-url.
    :param list[str] urls: The URLs of the to-be-cached files.
    """
    file_cache: dict[str, FileCacheInfo] = {}  # to be result
    directory = Path(directory)

    # add provided file-infos to `file_cache`
    for url, path in provided_filepaths_by_url.items():
        hash_ = hashlib.md5()
        path = Path(path)
        with path.open(mode="rb", buffering=1024 * 1024) as file:
            for chunk in file:
                hash_.update(chunk)
        file_cache[url] = FileCacheInfo(hash_md5=hash_.hexdigest(), path=path)

    # get other file-infos from internet
    with requests.Session() as session:
        for idx, url in enumerate(urls):
            if url not in provided_filepaths_by_url:
                hash_ = hashlib.md5()
                filepath = directory.joinpath(str(idx))
                with session.get(url, stream=True) as response, filepath.open(
                    mode="wb", buffering=1024 * 1024
                ) as file:
                    response.raise_for_status()

                    for chunk in response.iter_content(chunk_size=1024 * 1024):
                        hash_.update(chunk)
                        file.write(chunk)
                file_cache[url] = FileCacheInfo(hash_md5=hash_, path=filepath)

    # return
    return file_cache


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

    return TaskLog(pid=pid, previous_json=previous_json, json=json_)


def link_up(whole: TaskLog, part: TaskLog):
    """If unlinked, link jsons within `whole`, `part`."""
    whole_metadata = LOMMetadata(whole.json)
    part_metadata = LOMMetadata(part.json)

    whole_metadata.append_relation(part.pid, kind="haspart")
    part_metadata.append_relation(whole.pid, kind="ispartof")

    whole.json = whole_metadata.json
    part.json = part_metadata.json


def update_course_metadata(course_item: TaskLog):
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


def update_unit_metadata(unit_item: TaskLog):
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


def update_file_metadata(file_item: TaskLog):
    metadata = LOMMetadata(file_item.json or {}, overwritable=True)
    file_json = file_item.moodle_file_json

    # multi-use input data
    language = file_json["language"]

    # convert title
    if title := file_json["title"]:
        metadata.set_title(title, language_code=language)

    # convert language
    metadata.append_language(language)

    # abstract
    if abstract := html.unescape(file_json["abstract"]):
        metadata.append_description(abstract, language_code=language)

    # convert tags
    for tag in file_json["tags"]:
        if tag:
            metadata.append_keyword(tag, language_code=language)

    # convert persons
    for person in file_json["persons"]:
        name = f"{person['firstname']} {person['lastname']}"
        metadata.append_contribute(name, role=person["role"])

    # convert timereleased
    timereleased = file_json["timereleased"]
    datetime_obj = datetime.fromtimestamp(int(timereleased))
    datetime_isoformat = str(datetime_obj.date().isoformat())
    metadata.set_datetime(datetime_isoformat)

    # convert mimetype
    mimetype = file_json["mimetype"]
    metadata.append_format(mimetype)

    # convert filesize
    filesize = file_json["filesize"]
    metadata.set_size(filesize)

    # convert resourcetype
    # https://skohub.io/dini-ag-kim/hcrt/heads/master/w3id.org/kim/hcrt/slide.en.html
    resourcetype = file_json["resourcetype"]
    learningresourcetype_by_resourcetype = {
        "No selection": None,
        "Presentationslide": "slide",
    }
    if learningresourcetype := learningresourcetype_by_resourcetype[resourcetype]:
        metadata.append_learningresourcetype(learningresourcetype)

    # convert license
    license_url = file_json["license"]["source"]
    metadata.set_rights_url(license_url)

    # convert classification
    oefos_ids = [
        value["identifier"]
        for classification in file_json["classification"]
        for value in classification["values"]
    ]
    oefos_ids.sort(key=lambda id_: (-len(id_), id_))
    # metadata.append_oefos(oefos_ids)
    for id_ in oefos_ids:
        metadata.append_oefos_id(id_)
        metadata.append_oefos_id(id_, "en")
    metadata.append_oefos_id("1010")
    metadata.append_oefos_id("101001")
    metadata.append_oefos_id("1010")

    file_item.json = metadata.json


def insert_moodle_into_db(
    moodle_data: dict,
    filepaths_by_url: dict[str, Path] = None,
) -> None:
    """Insert data encoded in `moodle-data` into invenio-database.

    :param dict moodle_data: The data to be inserted into database,
        whose format matches `MoodleSchema`
    :param dict filepaths_by_url: A dictionary
        that maps some file-urls within `moodle_data` to filepaths.
        When a file-url is found in `filepaths_by_url`,
        the file on the corresponding filepath is used.
        Otherwise the file is downloaded from file-url.
    """
    # TODO: refactor: split up this function
    # TODO: link with previous course

    # validate input
    moodle_data = MoodleSchema().load(moodle_data)
    moodle_file_jsons = [
        file_json
        for moodlecourse in moodle_data["moodlecourses"]
        for file_json in moodlecourse["files"]
    ]

    # TODO: uniqueness-check, add these to schema
    # -> each url only once
    # -> each courseid has same json

    # download unprovided urls, built `file_cache`
    with TemporaryDirectory() as temp_dir:
        temp_dir = Path(temp_dir)
        file_cache: dict[str, FileCacheInfo] = cache_files(
            directory=temp_dir,
            provided_filepaths_by_url=filepaths_by_url,
            urls=[moodle_file_jsn["fileurl"] for moodle_file_jsn in moodle_file_jsons],
        )

        # initialize
        task_logs: dict[Key, TaskLog] = {}  # keep track of one log per course/unit/file

        # prepare: gather necessary information, create records if no previous versions exist
        for moodle_file_json in moodle_file_jsons:
            url = moodle_file_json["fileurl"]
            hash_md5 = file_cache[url].hash_md5
            file_key = FileKey(hash_md5=hash_md5, url=url)
            task_logs[file_key] = fetch_else_create(
                file_key.to_string_key(),
                resource_type="file",
            )
            for moodle_course_json in moodle_file_json["courses"]:
                unit_key = UnitKey.from_json(moodle_file_json, moodle_course_json)
                course_key = CourseKey.from_json(moodle_course_json)

                if unit_key not in task_logs:
                    unit_item = fetch_else_create(
                        unit_key.to_string_key(),
                        resource_type="unit",
                    )
                    unit_item.moodle_file_json = moodle_file_json
                    unit_item.moodle_course_json = moodle_course_json
                    task_logs[unit_key] = unit_item

                if course_key not in task_logs:
                    course_item = fetch_else_create(
                        course_key.to_string_key(),
                        resource_type="course",
                    )
                    course_item.moodle_file_json = moodle_file_json
                    course_item.moodle_course_json = moodle_course_json
                    task_logs[course_key] = course_item


        # TODO: append files to records
        # TODO: keep as invariant: only at most one file in db per md5-hash
        ...

    # link records
    # TODO: compute links here instead of above...
    # TODO: C: how to link previous courses?
    # TODO: is (course_log, 'haspart', pid) better?
    links: set[tuple[Key, str, str]] = set()  # {(course_key, "haspart", pid), ...}
    # links.add((course_key, unit_key))
    for whole_key, part_key in links:
        link_up(task_logs[whole_key], task_logs[part_key])

    # update lom-jsons with info from moodle
    for key, item in task_logs.items():
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
    for item in task_logs.values():
        if item.previous_json != item.json:
            # json got updated, now update database with new json
            edit(id_=item.pid)  # ensure a draft exists
            update_draft(id_=item.pid, data=item.json)

    # publish created drafts
    # uow rolls back all `.publish`s if one fails as to prevent an inconsistent database-state
    publish = partial(service.publish, identity=system_identity)
    with UnitOfWork() as uow:
        for item in task_logs.values():
            if item.previous_json != item.json:
                publish(id_=item.pid, uow=uow)

        uow.commit()
