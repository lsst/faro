#!/usr/bin/env python
import argparse
import time
import json

from metric_pipeline_scripts import JobReporter

def main(repository, collection, metrics_package, spec):
    jr = JobReporter(repository, collection, metrics_package, spec)
    jobs = jr.run()
    for k, v in jobs.items():
        filename = f"{metrics_package}_{spec}_{k}_{time.time()}.json"
        with open(filename, 'w') as fh:
            json.dump(v.json, fh, indent=2, sort_keys=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=('Produce a Job object which can either be used '
                                                  'to build a local report or to ship to SQuaSH.'))
    parser.add_argument('repository', type=str,
                        help='Path to a valid gen3 repository')
    parser.add_argument('collection', type=str,
                        help='Collection to search for metric measurement values')
    parser.add_argument('--metrics_package', type=str, default="validate_drp",
                        help='Name of metrics package to load')
    parser.add_argument('--spec', type=str, default="design",
                        help='Spec level to apply: minimum, design, or stretch')

    args = parser.parse_args()
    main(args.repository, args.collection, args.metrics_package, args.spec)
