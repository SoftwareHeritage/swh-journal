swh-journal
===========

Persistent logger of changes to the archive, with publish-subscribe support.

It allows other ``swh`` components to push log-like event in a Kafka (e.g.
the ``swh-storage`` can log every added object in Kafka), or to consume these
events (e.g. the replayer mechanism used to replicate the archive in a mirror).



