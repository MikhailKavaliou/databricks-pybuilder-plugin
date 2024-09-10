#### databricks-pybuilder-plugin

The plugin is considered to be used to deploy assets to a Databricks environment.

The plugin is activated with the following command in your build.by:
> use_plugin('pypi:databricks_pybuilder_plugin')

It provides a set of tasks for uploading resources, workspaces, deploying jobs,
or installing locally built whl dependencies into a databricks cluster.

#### Deployment to Databricks
Automated deployment is implemented on the PyBuilder tasks basis.

The list of available tasks:

1. **export_workspace** - exporting a workspace.
   A workspace is considered to be a folder in Databricks holding a notebook, or a set of notebooks.

The task uploads the `src/main/scripts/` content into a Databricks workspace.
It overrides files of the same names and leaves other files as is.

By default, a git branch name is used as a nested folder of a workspace
for uploading the content into the Databricks workspace, if it's available.
The `default` folder is used otherwise.
Use the `branch` parameter to import a workspace in your own folder:
>pyb export_workspace -P branch={custom_directory}

The final output path would be `/team_folder/application_name/{custom_directory}` in this way.

Executing the command from a master branch
>pyb export_workspace

would upload the workspace files into `/team_folder/application_name/master`.

Here is the list of related deployment settings
| Property                      | Value |Description |
| ----------------------------- | ----- | ---------- |
| project_workspace_path        | `src/main/scripts/` | The path to a folder in the project tree holding notebooks. |
| remote_workspace_path         | `/team_folder/application_name/` | The Databricks folder the notebooks would be uploaded into from project_workspace_path. |

All of the properties could be overridden with a -P parameter.

Usage example:
>pyb export_workspace [-P env={env}] [-P branch={branch}]


Environment specific properties
Disabled by default.


2. **export_resources** - exporting resources into dbfs.
   Uploads resource files into dbfs if any. Existing files are to be overridden.

Here is the list of related deployment settings
| Property               | Value |Description |
| -----------------------| ----- | ---------- |
| project_resources_path | `src/main/resources/` | The path to the project resources. |

All of the properties could be overridden with a -P parameter.

Usage example:
>pyb export_resources [-P env={env}] [-P branch={branch}]

3. **install_library** - deploying an whl-archive to a Databricks cluster.
   Uploads an whl archive to a Volume, and re-attaches the library to a cluster by name.
   Re-installing a new library version triggers the cluster starting
   to uninstall old libraries versions and to install a new one.
   Repetitive installations of a library of the same version don't start the cluster and don't upload files to s3.
   The library is just re-attached to a cluster in this way.
   Installed libraries in other branches are not affected.
   Reinstalling a library in the same branch deleting all files under the same branch directory in AWS s3.
   If a Cloud Provider other than AWS is used, feel free to override the task using the Cloud specific API.

Here is the list of related deployment settings
| Property            | Value |Description |
| --------------------| ----- | ---------- |
| remote_cluster_name | `Test_cluster_name` | The name of a remote Databricks cluster the library to be installed to. |
| attachable_lib_path | `/Volume/` | The Volume path to a folder holding the whl archives (dependencies). |
| attachable_lib_s3_path | `s3://{bucket_name}/volume/libs/` | The Volume s3 path to a folder holding the whl archives (dependencies). |

All of the properties could be overridden with a -P parameter.

Usage example:
>pyb install_library

4. **deploy_to_cluster** - a full deployment to a cluster.
   Runs the `export_resources`, `export_workspace`, `install_library` in a row.

Usage example:
>pyb deploy_to_cluster

5. **deploy_job** - deploying a job to the Databricks by name.
   Please, make sure that the job is created on the Databricks side.

Executes `export_resources` and `export_workspace` tasks preliminarily.
Updates the existing job using a job definition file.
The definition file supports the jinja2 template syntax.
Please, check documentation for details: https://jinja.palletsprojects.com/en/2.11.x/

Here is the list of related deployment settings
| Property            | Value |Description |
| --------------------| ----- | ---------- |
| job_definition_path | `src/main/databricks/databricks_job_settings.json` | The project path to a job definition file. |

All of the properties could be overridden with a -P parameter.

Usage example:
>pyb deploy_job [-P env={env}] [-P branch={branch}]

5. **post_init** - initializing `env` and `branch` properties for further tasks.
Executed automatically as a dependent task.


#### To Run a notebook with a custom dependency
1. Build the whl-archive with the`pyb` command.

2. Deploy all the assets using the command `pyb deploy_to_cluster`.

3. Get to the target folder in the Databricks workspace.

4. Attach the notebook to a cluster and run the script.


#### All properties list
| Property                                      | Default Value                                                                                                          | Description                                                                                                                                                                                                                                                                                                                                                                        |
|-----------------------------------------------|------------------------------------------------------------------------------------------------------------------------|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| databricks_credentials                        | `{`<br/>`'dev': {'host': '', 'token': ''}`<br/>`'qa': {'host': '', 'token': ''}`<br/>`'prod': {'host': '', 'token': ''}`<br/>`}` | Please specify credentials in the dictionary format: host and token.                                                                                                                                                                                                                                                                                                               |
| default_environment                           | `dev`                                                                                                                  | There are 3 supported environments: `dev`, `qa` and `prod`.                                                                                                                                                                                                                                                                                                                        |
| project_workspace_path                        | `src/main/scripts/`                                                                                                    | The directory content is going to be uploaded into a databricks workspace.<br/>These are considered to be notebook scripts.                                                                                                                                                                                                                                                        |
| remote_workspace_path                         |                                                                                                                        | The databricks workspace that files in `project_workspace_path` are copied to.                                                                                                                                                                                                                                                                                                     |
| include_git_branch_into_output_workspace_path | `True`                                                                                                                 | The flag enables adding an extra directory with the `branch` name to the `remote_workspace_path`. Requires git to be installed.                                                                                                                                                                                                                                                    |
| enable_env_sensitive_workspace_properties     | `False`                                                                                                                | The flag enables environment properties chosen by the `env_config_workspace_path`.                                                                                                                                                                                                                                                                                                 |
| env_config_workspace_path                     | `environment-settings/{env}.py`                                                                                        | The path to a property file to be chosen as a env properties. By default `env` included into a file name is used to pick properties.                                                                                                                                                                                                                                               |
| env_config_name                               | `env`                                                                                                                  | The expected environment properties file name. The `env_config_workspace_path` will be copied to databricks workspace with name.                                                                                                                                                                                                                                                   |
| with_dbfs_resources                           | `False`                                                                                                                | The flage enables uploading resource files from the `project_resources_path` directory to databricks hdfs `dbfs_resources_path`.                                                                                                                                                                                                                                                   |
| project_resources_path                        | `src/main/resources/`                                                                                                  | The local directory path holding resource files to be copied (txt, csv etc).                                                                                                                                                                                                                                                                                                       |
| dbfs_resources_path                           |                                                                                                                        | The output hdfs directory on databricks environment holding resources.                                                                                                                                                                                                                                                                                                             |
| attachable_lib_path                           |                                                                                                                        | The output Volume directory on databricks environment holding a built dependency (whl-archive). It must start with the `/Volume/` prefix and point to an existing Volume instance.                                                                                                                                                                                                 |
| attachable_lib_s3_path                        |                                                                                                                        | The output AWS s3 directory mounted as a Databricks Volume.                                                                                                                                                                                                                                                                                                                        |
| clean_attachable_lib                          | False                                                                                                                  | The Databricks Volume libraries folder will be cleaned from stored files.                                                                                                                                                                                                                                                                                                          |
| attachable_lib_envs                           | `['dev']`                                                                                                              | The list of environments that requires a dependency attached to a databricks cluster. The dependency is preliminary must be uploaded to the `attachable_library_path`.                                                                                                                                                                                                             |
| use_aws_role                                  |                                                                                                                        | The name of AWS role to be used as a profile in the boto3 client. The client is used for uploading resources into a Volume.                                                                                                                                                                                                                                                        |
| cluster_init_timeout                          | `5 * 60`                                                                                                               | The timeout of waiting a databricks cluster while it changes its state (initiating, restarting etc).                                                                                                                                                                                                                                                                               |
| remote_cluster_name                           |                                                                                                                        | The name of a databricks cluster that dependency is attached to.                                                                                                                                                                                                                                                                                                                   |
| job_definition_path                           | 'src/main/databricks/job_settings.json'                                                                                | The path to a dataricks job configuration in a json format - https://docs.databricks.com/dev-tools/api/2.0/jobs.html. It supports Jinja template syntax in order to setup env sensitive properties. It also supports multiple jobs definitions - use a json array for that. the list of properties available by default: env, branch, remote_workspace_path, remote_workspace_path |
| deploy_single_job                             |                                                                                                                        | The name of a job to be deployed. If your databricks job config contains multiple definitions, you can deploy just one of these jobs specifying a name of the particular job.                                                                                                                                                                                                      |
| extra_rendering_args                          |                                                                                                                        | Custom properties to be populated in the job definition file. Use a dicionary as an argument. For example: `{'app_name': name}`.                                                                                                                                                                                                                                                   |
