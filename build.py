import fileinput
import re
import sys

from pybuilder.core import init, use_plugin, task, Author

use_plugin("python.core")
use_plugin("python.distutils")
use_plugin("python.unittest")
use_plugin('python.flake8')
use_plugin('python.install_dependencies')

# task which will be run if execute 'pyb' without arguments
default_task = ["clean", "install_dependencies", "analyze", "publish"]

name = 'databricks-pybuilder-plugin'
summary = 'Pybuilder plugin providing tasks for assets deployment.'
description = 'Provides utilities for assets deployment to Databricks based on databricks API.'
authors = [Author('Mikhail Kavaliou', 'killswitch@tut.by')]
version = '0.0.5.dev'


@init
def initialize(project):
    """Build setting"""
    project.set_property('distutils_commands', ['sdist', 'bdist_egg', 'bdist_wheel'])
    project.set_property('distutils_classifiers', ['Development Status :: 5 - Production/Stable'])
    project.set_property('distutils_readme_description', True)
    project.set_property('distutils_description_overwrite', True)
    project.set_property('source_dist_ignore_patterns', ['*.pyc', '.hg*', '.svn', '.CVS', '__pycache__'])
    """Test settings"""
    """Setup unit test: set pyspark dependency through py.test, pytest.ini.  disable default unittest plugin run"""
    project.set_property('verbose', True)
    project.set_property('run_unit_tests_propagate_stdout', True)
    project.set_property('run_unit_tests_propagate_stderr', True)

    """Style check settings"""
    project.set_property('flake8_break_build', True)
    # E501: Ignore warnings related with too long row (more than 120 symbols)
    project.set_property('flake8_ignore', 'E501')
    # Display flake8 warnings and errors in command line output.
    project.set_property('flake8_verbose_output', 'True')


@task("prepare_development", description="increments versioning for next dev release")
def prepare_development(project, logger):
    # This task should run in buildspec.yaml only!
    current_v = project.version
    matched_v = re.match(r'^(\d+)\.(\d+)\.(\d+)$', current_v)
    if matched_v:
        next_v = "{}.{}.{}.dev".format(matched_v.group(1), matched_v.group(2), int(matched_v.group(3)) + 1)
        logger.info('Preparing development:: {v1} to {v2}'.format(v1=current_v, v2=next_v))
        update_build_file_version(current_v, next_v)
    else:
        raise RuntimeError('Already dev version! To increment please run prepare_release first.')


@task("prepare_release", description="drops dev from version for new release build")
def prepare_release(project, logger):
    # This task should run in buildspec.yaml only!
    current_v = project.version
    if re.match(r'^\d+\.\d+\.\d+\.dev$', current_v):
        next_v = current_v[0:-4]
        logger.info('Preparing release:: from {v1} to {v2}'.format(v1=current_v, v2=next_v))
        update_build_file_version(current_v, next_v)
    else:
        raise RuntimeError('Already build version! Please run pyb prepare_development to increment versioning')


def update_build_file_version(old_version, new_version):
    for line in fileinput.input('build.py', inplace=True):
        # It's appeared that the print function is printing into the build.py itself in scope of the for loop,
        # so the sys.stdout.write() is used instead.
        sys.stdout.write(line.replace('version = "{}"'.format(old_version), 'version = "{}"'.format(new_version)))
