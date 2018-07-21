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

import psutil

from airflow.task.task_runner.base_task_runner import BaseTaskRunner
from airflow.utils.helpers import reap_process_group
import os
import sys


class BashTaskRunner(BaseTaskRunner):
    """
    Runs the raw Airflow task by invoking through the Bash shell.
    """
    def __init__(self, local_task_job):
        super(BashTaskRunner, self).__init__(local_task_job)

    def start(self):
        from airflow.bin.cli import CLIFactory
        pid = os.fork()
        if pid == 0:
            parser = CLIFactory.get_parser()
            args = parser.parse_args(self._command[1:])
            args.func(args)
            sys.exit(1)
        else:
            self.process = psutil.Process(pid)

        #self.process = self.run_command(['bash', '-c'], join_args=True)

    def return_code(self):
        try:
            return self.process.wait(timeout=10)
        except psutil.TimeoutExpired:
            return None

    def terminate(self):
        if self.process and psutil.pid_exists(self.process.pid):
            reap_process_group(self.process.pid, self.log)

    def on_finish(self):
        super(BashTaskRunner, self).on_finish()
