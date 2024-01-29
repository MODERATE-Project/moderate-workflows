from dagster import get_dagster_logger
from metadata.config.common import WorkflowExecutionError
from metadata.workflow.metadata import MetadataWorkflow
from metadata.workflow.profiler import ProfilerWorkflow
from metadata.workflow.workflow_output_handler import print_status


def run_metadata_workflow(workflow_config: dict):
    workflow = MetadataWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    print_status(workflow)
    workflow.stop()


def run_profiler_workflow(workflow_config: dict, log_instead_of_raise: bool = False):
    logger = get_dagster_logger()
    workflow = ProfilerWorkflow.create(workflow_config)
    workflow.execute()

    try:
        workflow.raise_from_status()
    except WorkflowExecutionError as ex:
        if log_instead_of_raise:
            logger.warning("Profiler workflow failed")
            logger.exception(ex)
        else:
            raise

    print_status(workflow)
    workflow.stop()
