#!/usr/bin/env python
# This file is part of faro.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

import argparse

# Main code now in lsst.verify; this version for backward-compatibility only.
from lsst.verify.bin.jobReporter import main


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=('Produce a Job object which can either be used '
                                                  'to build a local report or to ship to SQuaSH.'))
    parser.add_argument('repository', type=str,
                        help='Path to a valid gen3 repository')
    parser.add_argument('collection', type=str,
                        help='Collection to search for metric measurement values')
    parser.add_argument('--metrics_package', type=str, default="validate_drp",
                        help='Name of metrics package to load, defaults to validate_drp.')
    parser.add_argument('--spec', type=str, default="design",
                        help='Spec level to apply: minimum, design, or stretch')
    parser.add_argument('--dataset_name', type=str, default="rc2_subset",
                        help='Name of the dataset for which the report is being generated. '
                             'Defaults to rc2_subset.')

    args = parser.parse_args()
    main(args.repository, args.collection, args.metrics_package, args.spec, args.dataset_name)
