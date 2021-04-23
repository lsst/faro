.. _lsst.faro-design:

.. py:currentmodule:: lsst.faro

.. _lsst.faro-references:

References and prior art
========================

``lsst.faro`` builds on
 - The lsst.verify framework for computing data quality metrics,  described in `DMTN-098 <https://dmtn-098.lsst.io>`_ and `DMTN-057 <https://dmtn-057.lsst.io>`_

 - The validate_drp package, the Gen2 middleware based code for computing KPMs, as described in `DMTN-008 <https://dmtn-008.lsst.io>`_

.. _lsst.faro-design_goals:

Design goals
============

Here we outline the architecture and design concepts of ``lsst.faro``

- Enable the computation of scalar performance metrics for LSST

- Gen3 based

.. _lsst.faro-architecture:

Architecture
============

``faro`` is based on the :ref:``lsst.verify`` framework for computing key performance metrics.


.. _lsst.faro-package_organization:

Organization of the faro package
================================

Directory structure
-------------------

- Preparation:  produces an intermediate data product
- Measurement:  produces an lsst.verify.Measurement
- Summary:  takes collection of lsst.verify.Measurement objects as input and produces an ``lsst.verify.Measurement``


Naming conventions
------------------


