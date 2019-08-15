# -*- coding: utf-8 -*-
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""jsonschema for validating serialized DAG and operator."""

import json
import jsonschema
import pkgutil
from typing import Any, Dict, Optional, Collection
from typing_extensions import Protocol

class Validator(Protocol):
    def is_valid(self, instance) -> bool:
        """Check if the instance is valid under the current schema"""
        ...

    def validate(self, instance) -> None:
        """Check if the instance is valid under the current schema, raising validation error if not"""
        ...

    def iter_errors(self, instance) -> Collection[jsonschema.exceptions.ValidationError]:
        """Lazily yield each of the validation errors in the given instance"""
        ...


def load_dag_schema() -> Validator:
    schema = json.loads(pkgutil.get_data(__name__, 'schema.json'))
    jsonschema.Draft7Validator.check_schema(schema)
    return jsonschema.Draft7Validator(schema)
