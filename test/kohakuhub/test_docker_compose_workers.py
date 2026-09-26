"""The khub-worker service can run several replicas, chosen at startup."""

import importlib.util
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[2]
REPLICAS = "${KOHAKU_HUB_WORKER_REPLICAS:-1}"


def _load_generator():
    spec = importlib.util.spec_from_file_location(
        "generate_docker_compose", ROOT / "scripts" / "generate_docker_compose.py"
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _generated_compose(tmp_path):
    generator = _load_generator()
    template = tmp_path / "kohakuhub.conf"
    generator.generate_config_template(template)
    return yaml.safe_load(generator.generate_docker_compose(generator.load_config_file(template)))


def _example_compose():
    return yaml.safe_load((ROOT / "docker-compose.example.yml").read_text())


@pytest.mark.parametrize("source", ["example", "generated"])
def test_worker_replicas_are_set_at_startup(source, tmp_path):
    compose = _example_compose() if source == "example" else _generated_compose(tmp_path)
    worker = compose["services"]["khub-worker"]

    # A fixed container_name makes Compose refuse to scale the service.
    assert "container_name" not in worker
    assert worker["deploy"]["replicas"] == REPLICAS
    # Each replica drains its own tasks on shutdown.
    assert worker["stop_grace_period"] == "45s"
    assert worker["command"] == ["python", "/app/startup.py", "worker"]


def test_generated_worker_matches_the_example(tmp_path):
    keys = ("build", "deploy", "restart", "command", "stop_grace_period", "depends_on", "volumes")
    generated = _generated_compose(tmp_path)["services"]["khub-worker"]
    example = _example_compose()["services"]["khub-worker"]

    assert {key: generated[key] for key in keys} == {key: example[key] for key in keys}


def test_generator_config_file_path_produces_a_compose_file(tmp_path):
    # Neither the config file nor the prompts set s3_bucket; hub-api used to
    # raise KeyError instead of falling back like the LakeFS blockstore does.
    env = _generated_compose(tmp_path)["services"]["hub-api"]["environment"]

    assert "KOHAKU_HUB_S3_BUCKET=hub-storage" in env
