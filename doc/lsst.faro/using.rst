.. _faro_using:

.. py:currentmodule:: lsst.faro

.. _lsst.faro.getting_started:

Getting started
===============

``lsst.faro`` is part of the `LSST Science Pipelines <https://pipelines.lsst.io/>`_. If you are new to the LSST Science Pipelines, it may be helpful to begin with the `Getting started tutorial <https://pipelines.lsst.io/#getting-started>`_ and `installation instructions <https://pipelines.lsst.io/#installation>`_. 

If developing on Rubin computing facilities, a `shared version of the software stack <https://developer.lsst.io/services/software.html#shared-software-stack>`_ should be available for use.

.. _lsst.faro.running:

Running faro
============

- Running and building ``lsst.faro`` locally.
- Running faro on an reprocessed Gen3 repository at NCSA

``lsst.faro`` is can be run using `pipetask <https://pipelines.lsst.io/modules/lsst.ctrl.mpexec/pipetask.html>`_.

.. _lsst.faro.shared:

Shared Gen3 Data Repositories
-----------------------------

Information on shared Gen3 data repositories for data managed at NCSA can be found in `/repo/README.md`. See `DMTN-167 <https://dmtn-167.lsst.io/>`_ for more information on the organization of Gen3 data repositories.

.. warning:: When developing metrics in ``faro``, particular care should be taken when creating a new dataset type name associated with a metric. As noted in `DMTN-167 <https://dmtn-167.lsst.io/#naming-conventions-for-dataset-types>`_, the dataset type names are *global* with no implicit name spacing. This may change in the future; see `DM-29817 <https://jira.lsstcorp.org/browse/DM-29817>`_. When developing metrics, it is recommended to run on a local data repository rather than a shared Gen3 data repository in case metrics need to renamed or the `dimensions <https://pipelines.lsst.io/modules/lsst.daf.butler/dimensions.html>`_ associated with metric calculation need to be changed.

Example: rc2_subset
-------------------

Running ``faro`` on a small local dataset. The `rc2_subset <git@github.com:lsst-dm/rc2_subset.git>`_ is the smallest CI dataset for which all ``faro`` metrics can be run without error and produce meaingful results.

1. Set up ``rc2_subset`` following the instructions `here <https://pipelines.lsst.io/v/daily/getting-started/data-setup.html#downloading-the-sample-hsc-data>`_.

2. Set up ``faro`` package; see :ref:`setting up <lsst.faro.setting_up>`.
   
3. An example command (update the command)::
     
     pipetask run -b $RC2_SUBSET_DIR/SMALL_HSC/butler.yaml -p $FARO_DIR/pipelines/metrics_pipeline_matched.yaml -i u/$USER/single_frame -o u/$USER/faro_matched_visits_r --register-dataset-types -d "instrument='HSC' AND detector=42 AND band='r'"

     
Example: HSC RC2 dataset
------------------------

Running ``faro`` on a Gen3 repository at NCSA. The HSC RC2 data that is reprocessed monthly with the latest version of the Science Pipelines is a good example, see `DMTN-091<https://dmtn-091.lsst.io>`_. Information on the current status of HSC RC2 re-processing and latest runs can be found `here <https://jira.lsstcorp.org/browse/DM-26911>`_.

1. Set up ``lsst.faro`` package; see :ref:`setting_up <lsst.faro.setting_up>`.

2. An example command, in this case running metrics on the source catalog of single visit::
   
     pipetask --long-log run -b /repo/main/butler.yaml --register-dataset-types -p $FARO_DIR/pipelines/measurement/measurement_detector_table.yaml -d "visit=35892 AND skymap='hsc_rings_v1' AND instrument='HSC'" --output u/$USER/faro_test -i HSC/runs/RC2/w_2021_18/DM-29973 --timeout 999999

Use your username.
     
Example: DRP processing
-----------------------

``lsst.faro`` can be run together with other processing steps in a pipeline, e.g., as part of DRP processing.

  Examples of this functionality can be found in the `rc2_subset <https://github.com/lsst-dm/rc2_subset/blob/master/pipelines/DRP.yaml>`_ and `obs_lsst <https://github.com/lsst/obs_lsst/blob/master/pipelines/imsim/DRP.yaml>`_ packages. For instance, one could follow the steps `in this tutorial <https://pipelines.lsst.io/v/daily/getting-started/singleframe.html#running-single-frame-processing>`_ but substitute ``$RC2_SUBSET_DIR/pipelines/DRP.yaml#faro_singleFrame`` for ``$RC2_SUBSET_DIR/pipelines/DRP.yaml#singleFrame``.
    
.. _lsst.faro.adding_a_metric:

Adding a metric to faro
=======================

Before making contributions to faro, we recommend to consult the `LSST DM Developers Guide <https://developer.lsst.io/index.html>`_ as a general reference for software development in Rubin DM, and in particular, the best practices covered in the  `DM development workflow <https://developer.lsst.io/work/flow.html>`_.

Normative Science Verification Metrics
--------------------------------------

``lsst.faro`` is used for both science verification as well as scientific validation and charactization. 

Normative metrics are associated with science performance requirements defined in the `DMSR <https://ls.st/dmsr>`_, `OSS <https://ls.st/oss>`_, and `LSR <https://ls.st/lsr>`_ that will be verified by the Rubin Observatory Construction Project. If you are intending to implement a normative metric, please read the information below; for non-normative metrics skip to the next section.

1. Please contact the core development team by posting on the #rubinobs-science-verification Slack channel or by reaching out to one of the main developers. This will facilitate coordination and scheduling of work.

2. Review the detailed metric specification and algorithm definition. Detailed requirement specifications and associated test cases are being developed in the `LSST Verification and Validation (LVV) project <https://jira.lsstcorp.org/projects/LVV>`_ in JIRA. (For more systems engineering details, see the `LSST Verification & Validation Documentation <https://confluence.lsstcorp.org/pages/viewpage.action?pageId=100173626>`_ and `LSST Verification Architecture <https://confluence.lsstcorp.org/display/SYSENG/LSST+Verification+Architecture>`_.) 

.. _lsst.faro.planning_work:
   
Planning Work
-------------

1. `Create JIRA ticket <https://developer.lsst.io/work/flow.html#agile-development-with-jira>`_. ``faro`` has been tracking development using 6-month work cycles, i.e., JIRA epics. There is also a `backlog epic <https://jira.lsstcorp.org/browse/DM-29525>`_. When starting faro development, or making a bugfix, create a JIRA ticket. Include "faro" as a Component and set the team as "DM Science". It is recommended to contact the ``faro`` team to help everyone stay on the same page.

.. _lsst.faro.setting_up:
   
Setting Up
----------
   
1. Development can be done from the `Rubin Science Platform (RSP) notebook aspect <https://nb.lsst.io/>`_, `lsst-devl services <https://developer.lsst.io/services/lsst-devl.html>`_, or using `Docker image <https://pipelines.lsst.io/install/docker.html>`_ containing the Science Pipelines software. If using the RSP, suggest to read the `tutorial <https://nb.lsst.io/science-pipelines/development-tutorial.html>`_ on developing Science Pipelines in the notebook aspect.

2. Set up `Science Pipelines <https://pipelines.lsst.io/install/setup.html>`_::

     source /software/lsstsw/stack/loadLSST.bash
     setup lsst_distrib

The example above points to a `shared version of the software stack <https://developer.lsst.io/services/software.html#shared-software-stack>`_ on the GPFS file systems.
     
3. `Clone the faro repo <https://github.com/lsst/faro>`_::

     git clone https://github.com/lsst/faro.git

This is a local version of ``faro`` package to do development work.
     
4. Set up local version of the ``faro`` package. ::

    cd faro
    setup -k -r .

At this point you can verify that you are using your local version::

    eups list -s | grep faro

5. `Create a development branch <https://developer.lsst.io/work/flow.html#ticket-branches>`_::

    git checkout -b tickets/DM-NNNNN

All development should happen on ticket branches (and should have associated JIRA tickets). User branches (e.g., ``u/jcarlin/``) can be used for experimenting/testing.

Adding a Metric
---------------

1. Identify the analysis context. Review the associated connections, config, and task base classes for that analysis context to understand the in-memory python objects that will be passed to the ``run`` method of the metric measurement task and the configuration options. See :ref:`design concepts <lsst.faro.design_concepts>` for more information. Currently implemented analysis contexts are listed :ref:`here<lsst.faro.currently_implemented_analysis_contexts>`.

2. Implement Measurement task. This will be an instance of ``lsst.pipe.base.Task`` that performs the specific operations of a given metric. See ``NumSourcesTask`` defined in `BaseSubTasks.py <https://github.com/lsst/faro/blob/master/python/lsst/faro/base/BaseSubTasks.py>`_ for a simple example metric that returns the number of rows in an input source/object catalog. Additional examples of measurement tasks can be found in the ``python/lsst/faro/measurement`` directory of the package.
   
3. Implement unit tests. All algorithmic code used for metric computation should have associated unit tests. Examples can be found in the package ``tests`` directory.

4. Add metric to a pipeline yaml file. The pipeline yaml contains the configuration information to execute metrics. See `measurement_visit_table.yaml <https://github.com/lsst/faro/blob/master/pipelines/measurement/measurement_visit_table.yaml>` for an example that uses ``VisitTableMeasurementTask`` to count the number of rows in an input source/object catalog. Additional examples of pipeline files can be found in ``pipelines/measurement`` directory of the package.

5. Name the metric. Currently each metric is associated with separately named dataset type that is global (more info :ref:`here<lsst.faro.shared>`). To date, metric names have followed the pattern "metricvalue_{package}_{metric}" where the "package" and "metric" are given in the yaml configuration file. Metric naming conventions is an area of active development and it is recommended to contact the ``faro`` development team for up-to-date guidance.
   
Review
------

The following is brief summary of the steps for `Review preparation <https://developer.lsst.io/work/flow.html#review-preparation>`_.

1. `Push code <https://developer.lsst.io/work/flow.html#pushing-code>`_.

2. `Run unit tests with scons <https://developer.lsst.io/python/testing.html>`_. Run scons from the top level directory of the package. ::

     scons

3. `Build package documentation locally <https://developer.lsst.io/stack/building-single-package-docs.html>`_. From the top level package directory::

     package-docs build

4. `Run continuous Integration test with Jenkins <https://developer.lsst.io/work/flow.html#testing-with-jenkins>`_. Now that we have tested the package on its own, it is time to test integration with the rest of the Science Pipelines. When running the Jenkins test, the list of EUPS packages to build should include `lsst_distrib lsst_ci ci_hsc_gen3 ci_imsim`. The latter two EUPS packages will run CI tests that include executing ``faro`` on DRP products.

5. `Make the Pull Request <https://developer.lsst.io/work/flow.html#make-a-pull-request>`_.

6. `Follow code review steps <https://developer.lsst.io/work/flow.html#dm-code-review-and-merging-process>`_.

7. `Merge <https://developer.lsst.io/work/flow.html#merging>`_. Rebase if needed -- see `pushing code <https://developer.lsst.io/work/flow.html#pushing-code>`_.

..
  Exporting Metrics
  =================

  TODO
