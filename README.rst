===========================
Cask Tracker Application
===========================

Introduction
============

The Cask™ Data Application Platform (CDAP) is an integrated, open source application
development platform for the Hadoop ecosystem that provides developers with data and
application abstractions to simplify and accelerate application development.

Tracker
=======

Cask Tracker is one such application built by the team at Cask that provides the ability to track data ingested
either through Cask Hydrator or through a custom CDAP application and provide input to data governance processes on a cluster.
It includes this data about "the data":

- Metadata

  - Tags, properties, and schema for CDAP datasets and programs
  - System and user scopes

- Data Quality

  - Metadata including feed-level and field-level quality metrics of datasets

- Data Usage Statistics

  - Usage statistics of both datasets and programs

Getting Started
===============

Prerequisites
-------------
To use the 0.3.0 version of Tracker, you must have CDAP version 4.0.x or higher.

Audit Publishing to Kafka
-------------------------
The Tracker App contains a flow that subscribes to the TMS topic to which CDAP publishes
the audit updates. Before using this application, you should enable publishing of audit updates to
TMS by setting the ``audit.enabled`` option in your cdap-site.xml to ``true``.

Building Cask Tracker
---------------------
Build the Tracker directly from the latest source code by downloading and compiling::

  git clone https://github.com/caskdata/cask-tracker.git
  cd cask-tracker
  mvn clean package

After the build completes, you will have a JAR in the ``./target/`` directory.

You can build without running tests using ``mvn clean package -DskipTests``

Deploying Cask Tracker
----------------------
Step 1: Using the CDAP CLI, deploy the plugin::

  > load artifact target/tracker-<version>.jar

Step 2: Create an application configuration file based on the instructions below.

Step 3: Create a CDAP application using the configuration file::

  > create app TrackerApp tracker <version> USER appconfig.txt

Application Configuration File
------------------------------
Create an application configuration file that contains the Zookeeper Quorum (not required in Standalone mode).

Sample configuration file::

  {
    "config": {
      "auditLogConfig": {
        "zookeeperString": "hostname:2181/cdap"
      }
    }
  }

**Audit Log Config:**

This key contains a property map with:

Required Properties:

- ``zookeeperString``: Zookeeper Quorum string that is used by CDAP [required only in Distributed mode]

Optional Properties:

- ``topic``: TMS Topic to which CDAP audit updates are published; default is ``audit`` which
  corresponds to the default topic used in CDAP for audit log updates
- ``numPartitions``: Number of Kafka partitions; default is set to ``10``
- ``offsetDataset``: Name of the dataset where TMS offsets are stored; default is ``_auditOffset``
- ``limit``: Number of TMS audit messages to read in batch

Mailing Lists
-------------
CDAP User Group and Development Discussions:

- `cdap-user@googlegroups.com <https://groups.google.com/d/forum/cdap-user>`__

The *cdap-user* mailing list is primarily for users using the product to develop
applications or building plugins for appplications. You can expect questions from
users, release announcements, and any other discussions that we think will be helpful
to the users.

IRC Channel
-----------
CDAP IRC Channel: #cdap on irc.freenode.net


License and Trademarks
======================

Copyright © 2016 Cask Data, Inc.

Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
in compliance with the License. You may obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under the
License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
either express or implied. See the License for the specific language governing permissions
and limitations under the License.

Cask is a trademark of Cask Data, Inc. All rights reserved.
