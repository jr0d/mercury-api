# Copyright 2017 Ruben Quinones (ruben.quinones@rackspace.com)
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.

from mercury_api.inventory.views import (
    ComputerView,
    ComputerCountView,
    ComputerQueryView,
    ComputerBootStateView,
    ComputerBootScriptView
)
from mercury_api.active.views import (
    ActiveComputerView,
    ActiveComputerQueryView,
)
from mercury_api.rpc.views import (
    JobView,
    JobStatusView,
    JobTaskView,
    TaskView,
)

computer_view = ComputerView.as_view('computer')
active_computer_view = ActiveComputerView.as_view('active_computer')
job_view = JobView.as_view('job')

api_urls = [
    # Inventory url rules
    ('/api/inventory/computers/query',
     ComputerQueryView.as_view('computer_query')),
    ('/api/inventory/computers/count',
     ComputerCountView.as_view('computer_count')),
    ('/api/inventory/computers/boot/state/<mercury_id>',
     ComputerBootStateView.as_view('computer_boot_state')),
    ('/api/inventory/computers/boot/script/<mercury_id>',
     ComputerBootScriptView.as_view('computer_boot_script')),
    ('/api/inventory/computers', computer_view),
    ('/api/inventory/computers/<mercury_id>', computer_view),
    # Active computer url rules
    ('/api/active/computers/query',
     ActiveComputerQueryView.as_view('active_computer_query')),
    ('/api/active/computers', active_computer_view),
    ('/api/active/computers/<mercury_id>', active_computer_view),
    # RPC url rules
    ('/api/rpc/jobs/status/<job_id>', JobStatusView.as_view('job_status')),
    ('/api/rpc/jobs/tasks/<job_id>', JobTaskView.as_view('job_task')),
    ('/api/rpc/jobs', job_view),
    ('/api/rpc/jobs/<job_id>', job_view),
    ('/api/rpc/task/<task_id>', TaskView.as_view('task')),
]
