Command-line interface
======================

.. code-block:: bash

   ./manage.py --help

Commands
--------

collect
~~~~~~~

Collect data from various APIs.

.. code-block:: bash

   manage.py collect [--resume PAGE] {source,...}

--resume PAGE         Resume data collection from this page

label
~~~~~

Manually label data as being about procurement.

.. code-block:: bash

   manage.py collect [--relabel] {source,...} [id ...]

--relabel             Relabel the records provided by ID

train
~~~~~

Train and cross-validate a classifier to label text about procurement.

.. code-block:: bash

   manage.py train [--language {english,...}] {source,...}

--language LANGUAGE   The language of the stopwords to use

.. note::

   To do a grid search, modify the ``PARAM_GRID`` constant in the code.

.. tip::

   To see progress during grid search, set: ``-v3``

predict
~~~~~~~

Automatically label data as being about procurement.

.. code-block:: bash

   manage.py predict {source,...}
