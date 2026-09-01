from unittest.mock import Mock

from airflow.task.trigger_rule import TriggerRule

from aircan.dags import pipeline_ckan_to_bigquery as pipeline


def test_update_ckan_resource_task_stamps_config(monkeypatch):
    config = {"resource": {"id": "resource-id"}}
    monkeypatch.setattr(pipeline, "_get_task_context", Mock(return_value=(config, {})))
    stamp = Mock()
    monkeypatch.setattr(pipeline, "update_resource_last_modified", stamp)

    pipeline.update_ckan_resource_task.function()

    stamp.assert_called_once_with(config)


def test_update_ckan_resource_runs_only_after_successful_write_branch():
    task = pipeline.dag.get_task("update_ckan_resource_task")

    assert task.trigger_rule == TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    assert task.upstream_task_ids == {
        "replace_or_append_table_task",
        "upsert_table_task",
    }
    assert task.downstream_task_ids == {"export_and_publish_task"}
