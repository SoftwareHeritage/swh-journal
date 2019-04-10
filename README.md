swh-journal
===========

Persistent logger of changes to the archive, with publish-subscribe support.

See the
[documentation](https://docs.softwareheritage.org/devel/swh-journal/index.html#software-heritage-journal)
for more details.

# Local test

As a pre-requisite, you need a kakfa installation path.
The following target will take care of this:

```
make install
```

Then, provided you are in the right virtual environment as described
in the [swh getting-started](https://docs.softwareheritage.org/devel/developer-setup.html#developer-setup):

```
pytest
```

or:

```
tox
```


# Running

## publisher

Command:
```
$ swh-journal --config-file ~/.config/swh/journal/publisher.yml \
              publisher
```

# Auto-completion

To have the completion, add the following in your
~/.virtualenvs/swh/bin/postactivate:

```
eval "$(_SWH_JOURNAL_COMPLETE=$autocomplete_cmd swh-journal)"
```
