.. _faro_using:

.. py:currentmodule:: lsst.faro

.. _lsst.faro.getting_started:

Getting started
===============

If you are new to the LSST science pipelines, you should first install the LSST science pipelines following one of the methods described at The LSST Science Pipelines -- Installation (add link).

.. _lsst.faro-running:

Running faro
============


- Running and building ``lsst.faro`` locally.
- Running faro on an reprocessed Gen3 repository at NCSA

.. _lsst.faro-adding_a_metric:

Adding a metric to faro
=======================

Before making contributions to faro, we recommend to consult the `LSST DM Developers Guide <https://developer.lsst.io/index.html>`_ as a general reference for software development in Rubin DM, and in particular, the best practices covered in the  `DM development workflow <https://developer.lsst.io/work/flow.html>`_.

Setting Up
----------

1. Identify the metric to be implemented. Examples of normative requirements related to science performance to be verified by the Rubin Construction Project can be found in the `DMSR <https://ls.st/dmsr>`_, `OSS <https://ls.st/oss>`_, and `LSR <https://ls.st/lsr>`_. faro also includes many non-normative science validation metrics.

2. For normative requirements, review the detailed metric specification and algorithm definition. Detailed requirement specifications and associated test cases are being developed in the `LSST Verification and Validation (LVV) project <https://jira.lsstcorp.org/projects/LVV>`_ in JIRA. (For more systems engineering details, see the `LSST Verification & Validation Documentation <https://confluence.lsstcorp.org/pages/viewpage.action?pageId=100173626>`_ and `LSST Verification Architecture <https://confluence.lsstcorp.org/display/SYSENG/LSST+Verification+Architecture>`_.) 

3. `Create JIRA ticket <https://developer.lsst.io/work/flow.html#agile-development-with-jira>`_. faro has been tracking development using 6-month work cycles, i.e., JIRA epics. There is also a `backlog epic <https://jira.lsstcorp.org/browse/DM-29525>`_. When starting faro development, or making a bugfix, create a JIRA ticket. Include "faro" as a Component and set the team as "DM Science".

4. Development can be done from the `Rubin Science Platform (RSP) notebook aspect <https://nb.lsst.io/>`_, `lsst-devl services <https://developer.lsst.io/services/lsst-devl.html>`_, or using `Docker image <https://pipelines.lsst.io/install/docker.html>`_ containing the Science Pipelines software. If using the RSP, suggest to read the `tutorial <https://nb.lsst.io/science-pipelines/development-tutorial.html>`_ on developing Science Pipelines in the notebook aspect.

5. Set up `Science Pipelines <https://pipelines.lsst.io/install/setup.html>`_.::

     source /software/lsstsw/stack/loadLSST.bash
     setup lsst_distrib
   
6. `Clone the faro repo <https://github.com/lsst/faro>`_::

     git clone https://github.com/lsst/faro.git
     cd faro

7. Set up the package. ::

    setup -k -r .

At this point you can verify that you are using your local version::

    eups list -s | grep faro

8. `Create a development branch <https://developer.lsst.io/work/flow.html#ticket-branches>`_::

    git checkout -b git checkout -b tickets/DM-NNNNN

Adding a Metric
---------------

1. Identify the analysis context. Review the associated connections, config, and task base classes for that analysis context to understand the in-memory python objects that will be passed to the ``run`` method of the metric measurement task and the configuration options. See design for more information.

Currently implemented analysis contexts include...

2. Implement Measurement task. This will be an instance of ``lsst.pipe.base.Task`` that performs the specific operations of a given metric. See ``NumSourcesTask`` defined in `BaseSubTasks.py <https://github.com/lsst/faro/blob/master/python/lsst/faro/base/BaseSubTasks.py>`_ for a simple example metric that returns the number of rows in an input source/object catalog.
   
3. Implement unit tests. All algorithmic code used for metric computation should have associated unit tests. Examples can be found in the package ``tests`` directory.

4. Add metric to a pipeline yaml file. The pipeline yaml contains the configuration information to execute metrics. See `measurement_visit_table.yaml <https://github.com/lsst/faro/blob/master/pipelines/measurement/measurement_visit_table.yaml>` for an example that uses ``VisitTableMeasurementTask`` to count the number of rows in an input source/object catalog.
   
Review
--------------------

The following is brief summary of the steps for `Review preparation <https://developer.lsst.io/work/flow.html#review-preparation>`_.

1. `Push code <https://developer.lsst.io/work/flow.html#pushing-code>`_.

2. `Run unit tests with scons <https://developer.lsst.io/python/testing.html>`_. Run scons from the top level directory of the package. ::

     scons

3. `Build package documentation locally <https://developer.lsst.io/stack/building-single-package-docs.html>`_. From the top level package directory::

     package-docs build

4. `Run continuous Integration test with Jenkins <https://developer.lsst.io/work/flow.html#testing-with-jenkins>`_. Now that we have tested the package on its own, it is time to test integration with the rest of the Science Pipelines. When running Jenkins test, the list of EUPS packages to build should include `lsst_distrib lsst_ci ci_hsc_gen3 ci_imsim`. The latter two EUPS packages will run CI tests that includes executing faro on DRP products.

5. `Make the Pull Request <https://developer.lsst.io/work/flow.html#make-a-pull-request>`_.

6. `Follow code review steps <https://developer.lsst.io/work/flow.html#dm-code-review-and-merging-process>`_.

7. `Merge <https://developer.lsst.io/work/flow.html#merging>`_. Rebase if needed -- see `pushing code <https://developer.lsst.io/work/flow.html#pushing-code>`_.


