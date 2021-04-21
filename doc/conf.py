
"""Sphinx configuration file for an LSST stack package.
This configuration only affects single-package Sphinx documentation builds.
"""

from documenteer.sphinxconfig.stackconf import build_package_configs
import lsst.faro.version as ver


_g = globals()
_g.update(build_package_configs(
    project_name='faro',
    version=ver.__version__))