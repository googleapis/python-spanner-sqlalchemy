# Copyright 2024 Google LLC All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from os import listdir
from os.path import isfile, join

import nox


@nox.session()
def hello_world(session):
    _sample(session)


@nox.session()
def _all_samples(session):
    _sample(session)

def _sample(session):
    session.install("testcontainers")
    session.install("sqlalchemy")
    session.install("setuptools")
    session.install(
        "git+https://github.com/googleapis/python-spanner.git#egg=google-cloud-spanner"
    )
    session.install("../.")
    if session.name == "_all_samples":
        files = [f for f in listdir(".") if isfile(join(".", f)) and f.endswith("_sample.py")]
        for file in files:
            session.run("python", file)
    else:
        session.run("python", session.name + "_sample.py")
