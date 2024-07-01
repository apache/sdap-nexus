.. _release:

**********************************
Release Candidate Evaluation Guide
**********************************

The following guide is intended for SDAP PMC members as instruction for evaluating release candidates. Non-PMC members should
also feel free to evaluate candidate releases, though their inputs on release VOTEs are considered advisory and are non-binding.
SDAP encourages its whole community to participate in discussion regardless.

Download & Verify Release Candidate
===================================

Follow the download link in the VOTE email and download all files in the release candidate directory
(``https://dist.apache.org/repos/dist/dev/sdap/apache-sdap-<version>-rc<candidate_number>/*``).

Verify Checksums
----------------

To verify checksums, for each ``.tar.gz`` file in the RC:

.. code-block:: bash

  shasum -a 512 <release-file>.tar.gz | cat - <release-file>.tar.gz.sha512

This will produce a SHA-512 checksum of the release file printed atop the expected checksum from the release for an easy
visual comparison.

For each of these, you should also check the checksums against the checksums provided in the VOTE email.

Verify Signatures
-----------------

To verify signatures, for each ``.tar.gz`` file in the RC:

.. code-block:: bash

  gpg --verify <release-file>.tar.gz.asc <release-file>.tar.gz.sha512

The expected output should be something similar to

.. code-block::

  gpg: Signature made Mon Jun 10 14:32:40 2024 PDT
  gpg:                using RSA key 4E98C4A32026656E14E0B570FC20035A010E3B7B
  gpg: Good signature from "Riley Kuttruff (CODE SIGNING KEY) <rkk@apache.org>" [ultimate]

The name and email of the signing key should correspond to the name and email that initiated the VOTE thread, and the key
MUST be in the `KEYS file <https://downloads.apache.org/sdap/KEYS>`_, which should be linked in the email and also available
through the `official SDAP Downloads page <https://sdap.apache.org/downloads>`_.

Build and Check Images
======================

Image Builds
------------

Follow the :ref:`Build Guide<build>` to build the SDAP Docker Images.

Check the Images
----------------

It's a requirement that ASF releases be free of code that is under `certain 3rd-party licenses <https://www.apache.org/legal/resolved.html>`_,
so the images should be inspected to ensure they are free of any such dependencies.

We specifically check for Python packages in the sdap-solr-init, sdap-collection-manager, sdap-granule-ingester and sdap-nexus-webapp
images:

.. code-block:: bash

  $ docker run --rm --entrypoint /bin/bash <image> -c 'pip install -q "pip-licenses<4.0" && pip-licenses'

.. note::

  For the sdap-solr-init image, replace ``pip-licenses<4.0`` in the above command with ``pip-licenses``.

Verify the packages do not include any GPL/LGPL licenses.

Acceptable licenses for a binary:

* Apache
* MIT
* BSD-2 / BSD-3
* MPL
* Python Software Foundation License
* HPND (for Pillow)
* OSI approved (for netCDF4)

Some licenses may be reported as UNKNOWN, this is ok if the package name is

* sdap-collection-manager
* sdap-ingester-common
* nexusproto

Otherwise, this should be looked into further.

Any other licences not enumerated above should be checked at the link at the top of this section, any further questions
should be relayed to the `SDAP PMC <mailto:dev@sdap.apache.org>`_.

Testing the Images
==================

Minimum Test
------------

Verify the images are working by using them in the :ref:`Quickstart Guide<quickstart>`.

Extended Testing
----------------

Section coming soon...

Vote
====

Draft a response to the VOTE thread (`guide on ASF voting <https://www.apache.org/foundation/voting.html>`_).

It is important you include what you checked/verified and, if applicable, what issues you found. **Do not just vote +1 or
-1 without any reasoning!**

Send your completed response.

This completes the release candidate evaluation process.
