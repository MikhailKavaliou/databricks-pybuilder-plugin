import fileinput
import os
import re
import time
import sys
import json

import boto3

from pathlib import Path
from jinja2 import Template

from databricks_cli.clusters.api import ClusterApi
from databricks_cli.dbfs.api import DbfsApi
from databricks_cli.dbfs.dbfs_path import DbfsPath
from databricks_cli.jobs.api import JobsApi
from databricks_cli.libraries.api import LibrariesApi
from databricks_cli.sdk import ApiClient
from databricks_cli.workspace.api import WorkspaceApi

from pybuilder.core import init, task, depends
from pybuilder.utils import assert_can_execute


__author__ = 'Mikhail Kavaliou'


@init
def initialize(project):
    # runtime dependencies
    project.plugin_depends_on('databricks_cli')
    project.plugin_depends_on('Jinja2')
    project.plugin_depends_on('boto3')

    """Databricks deployment settings"""
    databricks_credentials = {'dev': {'host': '', 'token': ''},
                              'qa': {'host': '', 'token': ''},
                              'prod': {'host': '', 'token': ''}}
    project.set_property('databricks_credentials', databricks_credentials)
    project.set_property('default_environment', 'dev')
    project.set_property('include_git_branch_into_output_workspace_path', True)
    project.set_property('with_dbfs_resources', False)
    project.set_property('project_workspace_path', 'src/main/scripts/')
    project.set_property('enable_env_sensitive_workspace_properties', False)
    project.set_property('env_config_workspace_path', 'environment-settings/{env}.py')
    project.set_property('env_config_name', 'env')
    project.set_property('project_resources_path', 'src/main/resources/')
    project.set_property('job_definition_path', 'src/main/databricks/job_settings.json')
    project.set_property('attachable_lib_envs', ['dev'])
    project.set_property('clean_attachable_lib', False)
    project.set_property('cluster_init_timeout', 5 * 60)


@task('post_init', description='Initializing some settings basing on passed init state.')
def post_init(project, logger):
    default_env = project.get_property('default_environment')
    env = project.get_property('env', default_env).lower()
    project.set_property('env', env)

    assert_can_execute(["git", "--version"], prerequisite="git is installed.",
                       caller="databricks-pybuilder-plugin", env=None)
    project.set_property('branch', project.get_property('branch', get_active_branch_name()))


@task('export_workspace', description='Uploading local files to a databricks workspace.')
@depends('post_init')
def export_workspace(project, logger):
    env = project.get_property('env')
    logger.info(f'\nExporting the workspace to {env.upper()}...\n')

    remote_workspace_path = _build_remote_workspace_path(project)
    project_workspace_path = project.get_property('project_workspace_path')

    workspace_client = WorkspaceApi(_get_databricks_client(project.get_property('databricks_credentials').get(env)))
    workspace_client.mkdirs(workspace_path=remote_workspace_path)
    _upload_workspace_files(workspace_client, project_workspace_path, remote_workspace_path, logger)

    # handling configuration file depending on env
    enable_env_sensitive_workspace_properties = project.get_property('enable_env_sensitive_workspace_properties')
    env_config_workspace_path = project.get_property('env_config_workspace_path')
    env_config_name = project.get_property('env_config_name')
    if enable_env_sensitive_workspace_properties and env_config_workspace_path and env_config_name:
        full_project_config_path = project.expand_path(
            project_workspace_path + env_config_workspace_path.format(env=env))
        full_remote_config_path = '/'.join([remote_workspace_path, env_config_name])
        logger.info(f'Exporting the config to {full_remote_config_path}...')
        _upload_workspace_file(workspace_client, full_project_config_path, full_remote_config_path, logger)

    logger.info('\nThe workspace has been exported.\n')


def _build_remote_workspace_path(project):
    remote_workspace_path = project.get_property('remote_workspace_path')
    if remote_workspace_path is None:
        raise Exception('The "remote_workspace_path" property is not set...\n')

    branch = project.get_property('branch')
    remote_workspace_path = remote_workspace_path.format(env=project.get_property('env')).replace('/None', '')
    if project.get_property('include_git_branch_into_output_workspace_path', False):
        remote_workspace_path += f'/{branch}'

    return remote_workspace_path


def _upload_workspace_files(client, project_workspace_path, remote_workspace_path, logger):
    print(f'Scanning scripts folder: {project_workspace_path}...')
    with os.scandir(project_workspace_path) as entries:
        for entry in entries:
            project_path = entry.path
            remote_path = '/'.join([remote_workspace_path, os.path.splitext(entry.name)[0]])
            if entry.is_dir():
                client.mkdirs(workspace_path=remote_path)
                _upload_workspace_files(client, project_path, remote_path, logger)
            else:
                _upload_workspace_file(client, project_path, remote_path, logger)
    print(f'\nAll the workspace files have been uploaded into {remote_workspace_path}.\n')


def _upload_workspace_file(client, from_path, to_path, logger):
    language = 'PYTHON' if from_path.endswith('.py') else 'SCALA' if from_path.endswith('.scala ') else 'R' if from_path.endswith('.r') else 'SQL'
    client.import_workspace(
        source_path=from_path,
        target_path=to_path,
        fmt='SOURCE',
        language=language,
        is_overwrite=True,
        headers=None
    )
    logger.info(f'The file has been uploaded into {to_path}.')


def _upload_files_to_dbfs(client, project_resources_path, dbfs_resources_path, logger):
    logger.info(f'Creating remote directories: {dbfs_resources_path}...')
    client.mkdirs(DbfsPath(dbfs_resources_path))
    logger.info(f'Scanning resources folder: {project_resources_path}...')
    with os.scandir(project_resources_path) as entries:
        for entry in entries:
            project_path = entry.path
            remote_path = '/'.join([dbfs_resources_path, entry.name])
            if entry.is_dir():
                client.mkdirs(DbfsPath(remote_path))
                _upload_files_to_dbfs(client, project_path, remote_path, logger)
            else:
                client.cp(
                    recursive=True,
                    overwrite=True,
                    src=project_path,
                    dst=remote_path,
                    headers=None
                )
                logger.info(f'The {project_path} has bean uploaded.')
    logger.info(f'\nAll the resource files have been uploaded into {dbfs_resources_path}.\n')


@task('install_library', description='Installing a build whl archive as a dependency into a Databricks cluster.')
@depends('post_init')
def install_library(project, logger):
    """
    This task should be run to upload the whl-archive to a cluster.
    """
    logger.info('\nInstalling the library to a cluster...\n')
    if not project.name or project.name == '.':
        raise Exception('Specify the "name" attribute of the project in your build.py.')

    cluster_name = project.get_property('remote_cluster_name')
    if cluster_name is None:
        raise Exception('The "remote_cluster_name" property is not set...\n')

    library_s3_path = project.get_property('attachable_lib_s3_path')
    if library_s3_path is None:
        raise Exception('The "attachable_lib_s3_path" property is not set...\n')

    library_remote_path = project.get_property('attachable_lib_path')
    if library_remote_path is None:
        raise Exception('The "library_remote_path" property is not set...\n')

    env = project.get_property('env')
    branch = project.get_property('branch')
    db_client = _get_databricks_client(project.get_property('databricks_credentials').get(env))
    cluster_client = ClusterApi(db_client)
    cluster_id = cluster_client.get_cluster_id_for_name(cluster_name)

    archive_name = _upload_archive(library_s3_path.format(env=env, branch=branch),
                                   project.expand_path('$dir_dist'),
                                   project.get_property('clean_attachable_lib', False),
                                   project.get_property('use_aws_role'),
                                   logger)
    archive_path = '/'.join([library_remote_path.format(env=env, branch=branch).rstrip('/'), archive_name])

    libraries_client = LibrariesApi(db_client)
    _detach_old_lib_from_cluster(libraries_client, cluster_id, project, logger)
    cluster_init_timeout = project.get_property('cluster_init_timeout')
    _start_cluster(cluster_client, cluster_id, cluster_init_timeout, logger)
    _attach_lib_to_cluster(libraries_client, cluster_id, archive_path, logger)

    if [lib for lib in libraries_client.cluster_status(cluster_id)['library_statuses'] if
            lib['status'] == 'UNINSTALL_ON_RESTART']:
        cluster_client.restart_cluster(cluster_id)
        logger.info(f'\nThe the cluster "{cluster_name}" is restarting...')

    logger.info(f'\nThe library has been installed to the cluster "{cluster_name}".\n')


def _upload_archive(library_s3_path, project_dist_path, clean_attachable_lib, aws_profile, logger):
    logger.info('Searching a built archive...')
    project_path = os.path.join(project_dist_path, 'dist')

    lib_archive = next(os.scandir(project_path))
    project_path = lib_archive.path
    archive_name = lib_archive.name
    logger.info(f'Found the dist "{project_path}".')

    remote_path = library_s3_path.strip('/') if library_s3_path.endswith('/') else library_s3_path
    remote_path = '/'.join([remote_path, archive_name])
    logger.info(f'Uploading the file: {remote_path}...')

    if aws_profile:
        session = boto3.Session(profile_name=aws_profile)
        s3_client = session.client('s3')
        logger.info(f'Using {aws_profile} AWS profile...')
    else:
        s3_client = boto3.client('s3')
        logger.info('Using default AWS profile...')
    bucket_name = library_s3_path.split('/')[2]

    if clean_attachable_lib:
        prefix = library_s3_path.replace(f's3://{bucket_name}/', '')
        s3_directory_content_list = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix).get('Contents', [])

        if s3_directory_content_list:
            for content_item in s3_directory_content_list:
                if archive_name in content_item['Key']:
                    logger.info(f'The archive {archive_name} is already uploaded. Skipping...')
                    return archive_name
            delete_objects = {'Objects': [{'Key': file['Key']} for file in s3_directory_content_list]}
            s3_client.delete_objects(Bucket=bucket_name, Delete=delete_objects)

    s3_client.upload_file(Filename=project_path,
                          Bucket=bucket_name,
                          Key=remote_path.replace(f's3://{bucket_name}/', ''))

    return archive_name


def _start_cluster(cluster_client, cluster_id, init_timeout, logger):
    logger.info('Starting the cluster...')
    cluster_state = cluster_client.get_cluster(cluster_id)['state']
    logger.info(f'The cluster {cluster_id} is {cluster_state}...')

    if cluster_state == 'RUNNING':
        pass
    elif cluster_state == 'RESTARTING' or cluster_state == 'PENDING':
        logger.info(f'The cluster {cluster_id} is restarting. Waiting for a running state...')
    elif cluster_state == 'TERMINATING':
        logger.info(f'The cluster {cluster_id} is terminating. Waiting while it\'s shutting down...')
        start_time = time.time()
        while cluster_state != 'TERMINATED':
            time.sleep(3)
            cluster_state = cluster_client.get_cluster(cluster_id)['state']
            if time.time() - start_time > init_timeout:
                raise Exception(f'The cluster {cluster_id} hasn\'t been started in 5 minutes...')
        logger.info(f'Starting the cluster {cluster_id}...')
        cluster_client.start_cluster(cluster_id)
    elif cluster_state == 'TERMINATED':
        logger.info(f'Starting the cluster {cluster_id}...')
        cluster_client.start_cluster(cluster_id)
    else:
        raise Exception(f'The state of the cluster {cluster_id} cannot be handled: {cluster_state}.')

    start_time = time.time()
    while cluster_state != 'RUNNING':
        time.sleep(3)
        cluster_state = cluster_client.get_cluster(cluster_id)['state']
        if time.time() - start_time > init_timeout:
            raise Exception(f'The cluster {cluster_id} hasn\'t been started in 5 minutes...')

    logger.info(f'The cluster {cluster_id} has been started.')


def _detach_old_lib_from_cluster(client, cluster_id, project, logger):
    cluster_libraries = client.cluster_status(cluster_id).get('library_statuses', [])
    libraries_to_remove = []
    archive_name = project.name
    for library in cluster_libraries:
        library_definition = library['library']
        if archive_name in library_definition.get('whl', ''):
            logger.info(f'The library is going to be detached: {library_definition}')
            libraries_to_remove.append(library_definition)

    if libraries_to_remove:
        client.uninstall_libraries(cluster_id, libraries_to_remove)
        logger.info('The old libraries have been detached.')
    else:
        logger.info('No libraries to detach found.')


def _attach_lib_to_cluster(client, cluster_id, attachable_lib_path, logger):
    libraries = [
        {'whl': attachable_lib_path}
    ]
    client.install_libraries(cluster_id, libraries)
    logger.info(f'The library has been attached: {attachable_lib_path}')


@task('export_resources', description='Uploads resources into Databricks hdfs.')
@depends('post_init')
def export_resources(project, logger):
    env = project.get_property('env')
    logger.info(f'\nExporting resources to {env.upper()}...\n')

    if project.get_property('with_dbfs_resources', False):
        dbfs_resources_path_value = project.get_property('dbfs_resources_path')
        if dbfs_resources_path_value is None:
            raise Exception('The "dbfs_resources_path" property is not specified.'
                            'For example: dbfs:/FileStore/tables/project_name/resources/{env}')

        dbfs_resources_path = dbfs_resources_path_value.format(env=env)
        project_resources_path = project.get_property('project_resources_path')

        dbfs_client = DbfsApi(_get_databricks_client(project.get_property('databricks_credentials').get(env)))
        _upload_files_to_dbfs(dbfs_client, project_resources_path, dbfs_resources_path, logger)
    else:
        logger.info('\nNo resources are to be exported.'
                    ' Set the "with_dbfs_resources" property to True in order to upload resources.\n')


@task('deploy_to_cluster',
      description='Deploy all the assets and install a built whl archive to the databricks cluster.')
@depends('export_workspace', 'export_resources', 'install_library')
def deploy_to_cluster(project, logger):
    logger.info('\nAll the assets have been rolled out to a cluster.\n')


@task('deploy_job', description='Deploy the databricks job entirely using a job definition config.')
@depends('export_workspace', 'export_resources')
def deploy_job(project, logger):
    env = project.get_property('env')
    branch = project.get_property('branch')
    logger.info(f'\nDeploying the job to {env.upper()}...\n')

    library_s3_path = project.get_property('attachable_lib_s3_path')
    if library_s3_path is None:
        raise Exception('The "attachable_lib_s3_path" property is not set...\n')

    library_remote_path = project.get_property('attachable_lib_path')
    if library_remote_path is None:
        raise Exception('The "library_remote_path" property is not set...\n')

    databricks_credentials = project.get_property('databricks_credentials').get(env)
    db_client = _get_databricks_client(databricks_credentials)

    # the lib path is pointing to Volume for defined envs
    archive_name = _upload_archive(library_s3_path.format(env=env, branch=branch),
                                   project.expand_path('$dir_dist'),
                                   project.get_property('clean_attachable_lib', False),
                                   project.get_property('use_aws_role'),
                                   logger) if env in project.get_property('attachable_lib_envs') else 'N/A'

    if archive_name == 'N/A':
        logger.info(f'The archive_path /{env}/{branch}/{archive_name} would be ignored.')
    else:
        logger.info(f'The {archive_name} file was uploaded under /{env}/{branch}/ path.')

    archive_path = '/'.join([library_remote_path.format(env=env, branch=branch), archive_name]).replace('//', '/')

    rendering_args = {'env': env, 'branch': branch, 'archive_path': archive_path,
                      'remote_workspace_path': _build_remote_workspace_path(project)}
    extra_rendering_args = project.get_property('extra_rendering_args')
    if extra_rendering_args is not None and type(extra_rendering_args) == dict:
        rendering_args.update(extra_rendering_args)
    else:
        logger.info('No extra arguments for the job definition found.')

    job_definition_path = project.expand_path(project.get_property('job_definition_path'))
    job_definitions = _read_job_definition(job_definition_path, rendering_args)
    job_definitions_json = json.loads(job_definitions)
    # wrap a single definition into a list for multiple definitions support
    if type(job_definitions_json) == dict:
        job_definitions_json = [job_definitions_json]

    jobs_client = JobsApi(db_client)

    deploy_single_job = project.get_property('deploy_single_job')
    if deploy_single_job:
        logger.info(f'Deploying a single job: {deploy_single_job}...')

    for job_definition in job_definitions_json:
        job_name = job_definition.get('name') if 'name' in job_definition else job_definition.get('settings', []).get('name')
        if deploy_single_job and deploy_single_job != job_name:
            logger.info(f'Skipping the job deployment: {job_name}...')
            continue

        logger.info(f'Looking for the job: "{job_name}"...')
        databricks_host = databricks_credentials.get('host')
        job_id = _get_job_id_by_name(jobs_client, job_name, databricks_host)
        logger.info(f'Found the job: {databricks_host}/#job/{job_id}')

        new_job_definition = {
            'job_id': job_id,
            'new_settings': job_definition
        }
        jobs_client.reset_job(new_job_definition)

        logger.info(f'The job "{job_name}" has been updated.')


def _read_job_definition(job_definition_path, rendering_args):
    with open(job_definition_path, 'r') as file:
        raw_job_definition = file.read()

        template = Template(raw_job_definition)
        job_definition = template.render(rendering_args)
        return job_definition


def _get_job_id_by_name(jobs_client, job_name, databricks_host):
    jobs = jobs_client.list_jobs().get('jobs')

    found_jobs = [job for job in jobs if job.get('settings').get('name') == job_name]
    if not found_jobs:
        raise Exception(f'No {job_name} is found on the host {databricks_host}...')

    job_id = found_jobs[0]['job_id']

    return job_id


def _get_databricks_client(env_credentials):
    return ApiClient(host=env_credentials.get('host'),
                     token=env_credentials.get('token'))


def update_build_file_version(old_version, new_version):
    for line in fileinput.input("build.py", inplace=True):
        # It's appeared that the print function is printing into the build.py itself in scope of the for loop,
        # so the sys.stdout.write() is used instead.
        sys.stdout.write(line.replace('version = "{}"'.format(old_version), 'version = "{}"'.format(new_version)))


def get_active_branch_name():
    head_dir = Path(".") / ".git" / "HEAD"
    if head_dir.exists():
        with head_dir.open("r") as f:
            content = f.read().splitlines()
            for line in content:
                if line[0:4] == "ref:":
                    return line.partition("refs/heads/")[2]
    else:
        return 'default'


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
