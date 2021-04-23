.. py:currentmodule:: lsst.faro

.. _lsst.faro:

#########
lsst.faro
#########

.. toctree::

The ``lsst.faro`` module contains code for computing scientific performance metrics on the outputs of the LSST science pipelines.

Prior to the development of the Gen3 middleware, the Gen2-based  ``lsst.validate.drp`` package computed scientific performance metrics.
``lsst.faro`` is the new Gen3 middleware- and PipelineTask- based package for computing scientific performance metrics.
The algorithms implemented in ``validate_drp`` were ported as-is to run in ``lsst.faro``.
``validate_drp`` is now deprecated; all future development of metrics will be carried out in ``lsst.faro``.


.. _lsst.faro.design:

Design and Development
======================

.. toctree::
  :maxdepth: 1

  design.rst

.. _lsst.faro.using:

Using faro
==========

.. toctree::
  :maxdepth: 1

  using.rst

.. _lsst.faro.contributing:

Contributing
============

``lsst.faro`` is developed at https://github.com/lsst/faro.
You can find Jira issues for this module under the `faro <https://jira.lsstcorp.org/issues/?jql=project%20%3D%20DM%20AND%20component%20%3D%20faro>`_ component.

.. _lsst.faro.api-ref:

Python API reference
====================

.. automodapi:: lsst.faro
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.faro.base
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.faro.preparation
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.faro.measurement
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.faro.summary
   :no-main-docstr:
   :no-inheritance-diagram:

.. automodapi:: lsst.faro.utils
   :no-main-docstr:
   :no-inheritance-diagram:
