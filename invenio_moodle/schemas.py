# -*- coding: utf-8 -*-
#
# Copyright (C) 2022 Graz University of Technology.
#
# invenio-moodle is free software; you can redistribute it and/or modify
# it under the terms of the MIT License; see LICENSE file for more details.

"""Schemas for validating input from moodle."""

from marshmallow import Schema
from marshmallow.fields import List, Nested, String


class ClassificationValuesSchema(Schema):
    """Moodle classification-values schema."""

    identifier = String()
    name = String()


class ClassificationSchema(Schema):
    """Moodle classification schema."""

    type = String()
    url = String()
    values = List(Nested(ClassificationValuesSchema))


class CourseSchema(Schema):
    """Moodle course schema."""

    courseid = String()
    courselanguage = String()
    coursename = String()
    description = String()
    identifier = String()
    lecturer = String()
    objective = String()
    organisation = String()
    sourceid = String()
    structure = String()


class LicenseSchema(Schema):
    """Moodle license schema."""

    fullname = String()
    shortname = String()
    source = String()


class PersonSchema(Schema):
    """Moodle person schema."""

    firstname = String()
    lastname = String()
    role = String()


class FileSchema(Schema):
    """Moodle file schema."""

    abstract = String()
    classification = List(Nested(ClassificationSchema))
    context = String()
    courses = List(Nested(CourseSchema))
    filecreationtime = String()
    filesize = String()
    fileurl = String()
    language = String()
    license = Nested(LicenseSchema)
    mimetype = String()
    persons = List(Nested(PersonSchema))
    resourcetype = String()
    semester = String()
    tags = List(String())
    timereleased = String()
    title = String()
    year = String()


class MoodleCourseSchema(Schema):
    """Moodle moodlecourse schema."""

    files = List(Nested(FileSchema))


class MoodleSchema(Schema):
    """Moodle moodlecourses schema.

    Data coming from moodle should be in this format.
    """

    moodlecourses = List(Nested(MoodleCourseSchema))
