.. _journal_clients:

Software Heritage Journal clients
=================================

Journal client are processes that read data from the |swh| Journal,
in order to efficiently process all existing objects, and process new objects
as they come.

Some journal clients, such as :ref:`swh-dataset <swh-dataset>` only read
existing objects and stop when they are done.

Other journal clients, such as the :ref:`mirror <swh-storage>` are expected to
read constantly from the journal.

They can run in parallel, and the :mod:`swh.journal.client` module provides an
abstraction handling all the setup, so actual clients only consists in a single
function that takes :mod:`model objects <swh.model.model>` as parameters.

For example, a very simple journal client that prints all revisions and releases
to the console can be implemented like this:

.. literalinclude:: example-journal-client.py


Parallelization
---------------

A single journal client, like the one above, is sequential.
It can however run concurrently by running the same program multiple times.
Kafka will coordinate the processes so the load is shared across processes.

.. _journal-client-authentication:

Authentication
--------------

In production, journal clients need credentials to access the journal.
Once you have credentials, they can be configured by adding this to the ``config``::

   config = {
       "sasl.mechanism": "SCRAM-SHA-512",
       "security.protocol": "SASL_SSL",
       "sasl.username": "<username>",
       "sasl.password": "<password>",
   }

There are two types of client: privileged and unprivileged.
The former has access to all the data, the latter gets redacted authorship information,
for privacy reasons.
Instead, the ``name`` and ``email`` fields of ``author`` and ``committer`` attributes
of release and revision objects are blank, and their ``fullname`` is a SHA256 hash
of their actual fullname.
The ``privileged`` parameter to ``get_journal_client`` must be set accordingly.

Order guarantees and replaying
------------------------------

The journal client shares the ordering guarantees of Kafka.
The short version is that you should not assume any order unless specified otherwise in
the `Kafka documentation <https://kafka.apache.org/documentation/>`__,
nor that two related objects are sent to the same process.

We call "replay" any workflow that involves a journal client writing all (or most)
objects to a new database.
This can be either continuous (in effect, this produces a mirror database),
or one-off.

Either way, particular attention should be given to this lax ordering, as replaying
produces databases that are (temporarily) inconsistent, because some objects may
point to objects that are not replayed yet.

For one-off replays, this can be mostly solved by processing objects
in reverse topologic order:
as contents don't reference any object,
directories only reference contents and directories,
revisions only reference directories, etc. ;
this means that replayers can first process all revisions, then all directories,
then all contents.
This keeps the number of inconsistencies relatively small.

For continuous replays, replayed databases are eventually consistent.
