# databricks notebook source
import pytest
import sys

# skip writing pyc files on a readonly filesystem
sys.dont_write_bytecode = True

# run pytest
retcode = pytest.main([".", "-v", "-p", "no:cacheprovider"])

# fail the cell execution if there are any test failures
assert retcode == 0, "Tests failed or were not detected"