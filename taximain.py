'''
Author: Gábor Hraskó
Email: gabor@hrasko.com

This is the main module of the NY Taxi Trips stream analyzer demo project.
The program reads streaming data about NY state taxi rides from GCP PubSub 
topic and calculates various aggregated metrics.

Modules:

taxi.py: Configuration handling
pipeline.py: The DataFlow pipeline initialization and implementation
transform.py: The transformation routines
'''

from __future__ import absolute_import

import nytaxirides.taxi as taxi

# --------------------------------------------------------------------
# Running the pipeline ...

if __name__ == '__main__':

    taxi.run()
    print('\nNormal program termination.\n')