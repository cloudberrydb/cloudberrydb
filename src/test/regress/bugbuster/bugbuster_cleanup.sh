#!/bin/bash
## ======================================================================
## Execute bugbuster cleanup steps.
## ======================================================================

##
## Required for gpperfmon suite
##

gpconfig -c gp_enable_gpperfmon -v off

gpstop -ar
