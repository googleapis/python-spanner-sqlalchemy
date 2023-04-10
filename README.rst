Spanner dialect for SQLAlchemy
==============================

Spanner dialect for SQLAlchemy represents an interface API designed to
make it possible to control Cloud Spanner databases with SQLAlchemy API.
The dialect is built on top of `the Spanner DB
API <https://github.com/googleapis/python-spanner/tree/master/google/cloud/spanner_dbapi>`__,
which is designed in accordance with
`PEP-249 <https://www.python.org/dev/peps/pep-0249/>`__.

Known limitations are listed `here <#features-and-limitations>`__. All
supported features have been tested and verified to work with the test
configurations. There may be configurations and/or data model variations
that have not yet been covered by the tests and that show unexpected
behavior. Please report any problems that you might encounter by
`creating a new
issue <https://github.com/googleapis/python-spanner-sqlalchemy/issues/new>`__.

-  `Cloud Spanner product
   documentation <https://cloud.google.com/spanner/docs>`__
-  `SQLAlchemy product documentation <https://www.sqlalchemy.org/>`__

Quick Start
-----------

In order to use this package, you first need to go through the following
steps:

1. `Select or create a Cloud Platform
   project. <https://console.cloud.google.com/project>`__
2. `Enable billing for your
   project. <https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project>`__
3. `Enable the Google Cloud Spanner
   API. <https://cloud.google.com/spanner>`__
4. `Setup
   Authentication. <https://googleapis.dev/python/google-api-core/latest/auth.html>`__

Installation
------------
Stable released version of the package is available on PyPi:

::

   pip install sqlalchemy-spanner

To install an in-development version of the package, clone its
Git-repository:

::

   git clone https://github.com/googleapis/python-spanner-sqlalchemy.git

Next install the package from the package ``setup.py`` file:

::

   python setup.py install

During setup the dialect will be registered with entry points.

A Minimal App
-------------

Database URL
~~~~~~~~~~~~

In order to connect to a database one have to use its URL on connection
creation step. SQLAlchemy 1.3 and 1.4 versions have a bit of difference
on this step in a dialect prefix part:

.. code:: python

   # for SQLAlchemy 1.3:
   spanner:///projects/project-id/instances/instance-id/databases/database-id

   # for SQLAlchemy 1.4:
   spanner+spanner:///projects/project-id/instances/instance-id/databases/database-id

To pass your custom client object directly to be be used, create engine as following:

.. code:: python

    engine = create_engine(
        "spanner+spanner:///projects/project-id/instances/instance-id/databases/database-id",
        connect_args={'client': spanner.Client(project="project-id")}
    )

Create a table
~~~~~~~~~~~~~~

.. code:: python

   from sqlalchemy import (
       Column,
       Integer,
       MetaData,
       String,
       Table,
       create_engine,
   )

   engine = create_engine(
       "spanner:///projects/project-id/instances/instance-id/databases/database-id"
   )
   metadata = MetaData(bind=engine)

   user = Table(
       "users",
       metadata,
       Column("user_id", Integer, primary_key=True),
       Column("user_name", String(16), nullable=False),
   )

   metadata.create_all(engine)

Insert a row
~~~~~~~~~~~~

.. code:: python

   import uuid

   from sqlalchemy import (
       MetaData,
       Table,
       create_engine,
   )

   engine = create_engine(
       "spanner:///projects/project-id/instances/instance-id/databases/database-id"
   )
   user = Table("users", MetaData(bind=engine), autoload=True)
   user_id = uuid.uuid4().hex[:6].lower()

   with engine.begin() as connection:
       connection.execute(user.insert(), {"user_id": user_id, "user_name": "Full Name"})

Read
~~~~

.. code:: python

   from sqlalchemy import MetaData, Table, create_engine, select

   engine = create_engine(
       "spanner:///projects/project-id/instances/instance-id/databases/database-id"
   )
   table = Table("users", MetaData(bind=engine), autoload=True)

   with engine.begin() as connection:
       for row in connection.execute(select(["*"], from_obj=table)).fetchall():
           print(row)

Migration
---------

SQLAlchemy uses `Alembic <https://alembic.sqlalchemy.org/en/latest/#>`__
tool to organize database migrations.

Spanner dialect doesn't provide a default migration environment, it's up
to user to write it. One thing to be noted here - one should explicitly
set ``alembic_version`` table not to use migration revision id as a
primary key:

.. code:: python

   with connectable.connect() as connection:
       context.configure(
           connection=connection,
           target_metadata=target_metadata,
           version_table_pk=False,  # don't use primary key in the versions table
       )

As Spanner restricts changing a primary key value, not setting the ``version_table_pk`` flag
to ``False`` can cause migration problems. If ``alembic_versions`` table was already created with a primary key, setting the flag to ``False`` will not work, because the flag is only applied on table creation.    

Notice that DDL statements in Spanner are not transactional. They will not be automatically reverted in case of a migration fail. Also Spanner encourage use of the `autocommit_block() <https://alembic.sqlalchemy.org/en/latest/api/runtime.html#alembic.runtime.migration.MigrationContext.autocommit_block>`__ for migrations in order to prevent DDLs from aborting migration transactions with schema modifications.

| **Warning!**
| A migration script can produce a lot of DDL statements. If each of the
  statements is executed separately, performance issues can occur. To
  avoid it, it's highly recommended to use the `Alembic batch
  context <https://alembic.sqlalchemy.org/en/latest/batch.html>`__
  feature to pack DDL statements into groups of statements.

Features and limitations
------------------------

Interleaved tables
~~~~~~~~~~~~~~~~~~

| Cloud Spanner dialect includes two dialect-specific arguments for
  ``Table`` constructor, which help to define interleave relations:
  ``spanner_interleave_in`` - a parent table name
  ``spanner_inverleave_on_delete_cascade`` - a flag specifying if
  ``ON DELETE CASCADE`` statement must be used for the interleave
  relation
| An example of interleave relations definition:

.. code:: python

   team = Table(
       "team",
       metadata,
       Column("team_id", Integer, primary_key=True),
       Column("team_name", String(16), nullable=False),
   )
   team.create(engine)

   client = Table(
       "client",
       metadata,
       Column("team_id", Integer, primary_key=True),
       Column("client_id", Integer, primary_key=True),
       Column("client_name", String(16), nullable=False),
       spanner_interleave_in="team",
       spanner_interleave_on_delete_cascade=True,
   )
   client.add_is_dependent_on(team)

   client.create(engine)

**Note**: Interleaved tables have a dependency between them, so the
parent table must be created before the child table. When creating
tables with this feature, make sure to call ``add_is_dependent_on()`` on
the child table to request SQLAlchemy to create the parent table before
the child table.

Unique constraints
~~~~~~~~~~~~~~~~~~

Cloud Spanner doesn't support direct UNIQUE constraints creation. In
order to achieve column values uniqueness UNIQUE indexes should be used.

Instead of direct UNIQUE constraint creation:

.. code:: python

   Table(
       'table',
       metadata,
       Column('col1', Integer),
       UniqueConstraint('col1', name='uix_1')
   )

Create a UNIQUE index:

.. code:: python

   Table(
       'table',
       metadata,
       Column('col1', Integer),
       Index("uix_1", "col1", unique=True),
   )

Autocommit mode
~~~~~~~~~~~~~~~

Spanner dialect supports both ``SERIALIZABLE`` and ``AUTOCOMMIT``
isolation levels. ``SERIALIZABLE`` is the default one, where
transactions need to be committed manually. ``AUTOCOMMIT`` mode
corresponds to automatically committing of a query right in its
execution time.

Isolation level change example:

.. code:: python

   from sqlalchemy import create_engine

   eng = create_engine("spanner:///projects/project-id/instances/instance-id/databases/database-id")
   autocommit_engine = eng.execution_options(isolation_level="AUTOCOMMIT")

Automatic transactions retry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~
In the default ``SERIALIZABLE`` mode transactions may fail with ``Aborted`` exception. This is a transient kind of errors, which mostly happen to prevent data corruption by concurrent modifications. Though the original transaction becomes non operational, a simple retry of the queries solves the issue.

This, however, may require to manually repeat a long list of operations, executed in the failed transaction. To simplify it, Spanner Connection API tracks all the operations, executed inside current transaction, and their result checksums. If the transaction failed with ``Aborted`` exception, the Connection API will automatically start a new transaction and will re-run all the tracked operations, checking if their results are the same as they were in the original transaction. In case data changed, and results differ, the transaction is dropped, as there is no way to automatically retry it.

In ``AUTOCOMMIT`` mode automatic transactions retry mechanism is disabled, as every operation is committed just in time, and there is no way an ``Aborted`` exception can happen.

Autoincremented IDs
~~~~~~~~~~~~~~~~~~~

Cloud Spanner doesn't support autoincremented IDs mechanism due to
performance reasons (`see for more
details <https://cloud.google.com/spanner/docs/schema-design#primary-key-prevent-hotspots>`__).
We recommend that you use the Python
`uuid <https://docs.python.org/3/library/uuid.html>`__ module to
generate primary key fields to avoid creating monotonically increasing
keys.

Though it's not encouraged to do so, in case you *need* the feature, you
can simulate it manually as follows:

.. code:: python

   with engine.begin() as connection:
       top_id = connection.execute(
           select([user.c.user_id]).order_by(user.c.user_id.desc()).limit(1)
       ).fetchone()
       next_id = top_id[0] + 1 if top_id else 1

       connection.execute(user.insert(), {"user_id": next_id})

Query hints
~~~~~~~~~~~

Spanner dialect supports `query
hints <https://cloud.google.com/spanner/docs/query-syntax#table_hints>`__,
which give the ability to set additional query execution parameters.
Usage example:

.. code:: python

   session = Session(engine)

   Base = declarative_base()

   class User(Base):
       """Data model."""

       __tablename__ = "users"
       id = Column(Integer, primary_key=True)
       name = Column(String(50))


   query = session.query(User)
   query = query.with_hint(
       selectable=User, text="@{FORCE_INDEX=index_name}"
   )
   query = query.filter(User.name.in_(["val1", "val2"]))
   query.statement.compile(session.bind)

ReadOnly transactions
~~~~~~~~~~~~~~~~~~~~~

By default, transactions produced by a Spanner connection are in
ReadWrite mode. However, some applications require an ability to grant
ReadOnly access to users/methods; for these cases Spanner dialect
supports the ``read_only`` execution option, which switches a connection
into ReadOnly mode:

.. code:: python

   with engine.connect().execution_options(read_only=True) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

Note that execution options are applied lazily - on the ``execute()``
method call, right before it.

ReadOnly/ReadWrite mode of a connection can't be changed while a
transaction is in progress - first you must commit or rollback it.

Stale reads
~~~~~~~~~~~

To use the Spanner `Stale
Reads <https://cloud.google.com/spanner/docs/reads#perform-stale-read>`__
with SQLAlchemy you can tweak the connection execution options with a
wanted staleness value. For example:

.. code:: python

   # maximum staleness
   with engine.connect().execution_options(
       read_only=True,
       staleness={"max_staleness": datetime.timedelta(seconds=5)}
   ) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

.. code:: python

   # exact staleness
   with engine.connect().execution_options(
       read_only=True,
       staleness={"exact_staleness": datetime.timedelta(seconds=5)}
   ) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

.. code:: python

   # min read timestamp
   with engine.connect().execution_options(
       read_only=True,
       staleness={"min_read_timestamp": datetime.datetime(2021, 11, 17, 12, 55, 30)}
   ) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

.. code:: python

   # read timestamp
   with engine.connect().execution_options(
       read_only=True,
       staleness={"read_timestamp": datetime.datetime(2021, 11, 17, 12, 55, 30)}
   ) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

Note that the set option will be dropped when the connection is returned
back to the pool.

Request priority
~~~~~~~~~~~~~~~~~~~~~
In order to use Request Priorities feature in Cloud Spanner, SQLAlchemy provides an ``execution_options`` parameter:

.. code:: python

   from google.cloud.spanner_v1 import RequestOptions

   with engine.connect().execution_options(
       request_priority=RequestOptions.Priority.PRIORITY_MEDIUM
   ) as connection:
       connection.execute(select(["*"], from_obj=table)).fetchall()

DDL and transactions
~~~~~~~~~~~~~~~~~~~~

DDL statements are executed outside the regular transactions mechanism,
which means DDL statements will not be rolled back on normal transaction
rollback.

Dropping a table
~~~~~~~~~~~~~~~~

Cloud Spanner, by default, doesn't drop tables, which have secondary
indexes and/or foreign key constraints. In Spanner dialect for
SQLAlchemy, however, this restriction is omitted - if a table you are
trying to delete has indexes/foreign keys, they will be dropped
automatically right before dropping the table.

Data types
~~~~~~~~~~

Data types table mapping SQLAlchemy types to Cloud Spanner types:

========== =========
SQLAlchemy Spanner
========== =========
INTEGER    INT64
BIGINT     INT64
DECIMAL    NUMERIC
FLOAT      FLOAT64
TEXT       STRING
ARRAY      ARRAY
BINARY     BYTES
VARCHAR    STRING
CHAR       STRING
BOOLEAN    BOOL
DATETIME   TIMESTAMP
NUMERIC    NUMERIC
========== =========

Other limitations
~~~~~~~~~~~~~~~~~

-  WITH RECURSIVE statement is not supported.
-  Named schemas are not supported.
-  Temporary tables are not supported.
-  Numeric type dimensions (scale and precision) are constant. See the
   `docs <https://cloud.google.com/spanner/docs/data-types#numeric_types>`__.

Best practices
--------------

When a SQLAlchemy function is called, a new connection to a database is
established and a Spanner session object is fetched. In case of
connectionless execution these fetches are done for every ``execute()``
call, which can cause a significant latency. To avoid initiating a
Spanner session on every ``execute()`` call it's recommended to write
code in connection-bounded fashion. Once a ``Connection()`` object is
explicitly initiated, it fetches a Spanner session object and uses it
for all the following calls made on this ``Connection()`` object.

Non-optimal connectionless use:

.. code:: python

   # execute() is called on object, which is not a Connection() object
   insert(user).values(user_id=1, user_name="Full Name").execute()

Optimal connection-bounded use:

.. code:: python

   with engine.begin() as connection:
       # execute() is called on a Connection() object
       connection.execute(user.insert(), {"user_id": 1, "user_name": "Full Name"})

Connectionless way of use is also deprecated since SQLAlchemy 2.0 and
soon will be removed (see in `SQLAlchemy
docs <https://docs.sqlalchemy.org/en/14/core/connections.html#connectionless-execution-implicit-execution>`__).

Running tests
-------------

Spanner dialect includes a compliance, migration and unit test suite. To
run the tests the ``nox`` package commands can be used:

::

   # Run the whole suite
   $ nox

   # Run a particular test session
   $ nox -s migration_test

Running tests on Spanner emulator
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dialect test suite can be runned on `Spanner
emulator <https://cloud.google.com/spanner/docs/emulator>`__. Several
tests, relating to ``NULL`` values of data types, are skipped when
executed on emulator.

Contributing
------------

Contributions to this library are welcome and encouraged. Please report
issues, file feature requests, and send pull requests. See
`CONTRIBUTING <https://github.com/googleapis/python-spanner-sqlalchemy/blob/main/contributing.md>`__
for more information on how to get started.

**Note that this project is not officially supported by Google as part
of the Cloud Spanner product.**

Please note that this project is released with a Contributor Code of
Conduct. By participating in this project you agree to abide by its
terms. See the `Code of
Conduct <https://github.com/googleapis/python-spanner-sqlalchemy/blob/main/code-of-conduct.md>`__
for more information.
