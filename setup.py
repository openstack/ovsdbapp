# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at:
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import setuptools

VERSION = "0.0.1"

setup_args = dict(
    name='ovsdbapp',
    description='Library for creating OVSDB apps',
    version=VERSION,
    url='https://github.com/otherwiseguy/ovsdbapp/',
    author='Terry Wilson',
    author_email='twilson@redhat.com',
    packages=['ovsdbapp'],
    keywords=['openvswitch', 'ovs', 'OVSDB'],
    license='Apache 2.0',
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Topic :: Database :: Front-Ends',
        'Topic :: Software Development :: Libraries :: Python Modules',
        'Topic :: System :: Networking',
        'License :: OSI Approved :: Apache Software License',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
    ]
)

setuptools.setup(**setup_args)
