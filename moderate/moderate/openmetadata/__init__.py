from metadata.ingestion.api.workflow import Workflow
from metadata.profiler.api.workflow import ProfilerWorkflow


def run_workflow(workflow_config: dict):
    workflow = Workflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


def run_profiler_workflow(workflow_config: dict):
    workflow = ProfilerWorkflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()
