from pathlib import Path

import pytest

from otter.config.config import Config
from otter.config.models import CliSettings, EnvSettings, TaskDefinition, YamlSettings


@pytest.fixture
def c(mocker):
    mock_env = mocker.patch('otter.config.config.parse_env')
    mock_cli = mocker.patch('otter.config.config.parse_cli')
    mock_yaml = mocker.patch('otter.config.config.parse_yaml')
    mock_get_yaml_settings = mocker.patch('otter.config.config.get_yaml_settings')

    mock_env.return_value = EnvSettings(work_dir=Path('./somewhere'))
    mock_cli.return_value = CliSettings(step='step_1', config_file=Path('test_cli.yaml'))
    mock_yaml.return_value = {'steps': {'step_1': [{'name': 'task_1'}]}}
    mock_get_yaml_settings.return_value = YamlSettings(release_uri='gs://bucket/path/to/file')

    return Config()


def test_validate_step_valid(c):
    c = Config()
    c.load()

    c._validate_step()

    assert True


def test_validate_step_invalid(c):
    c = Config()
    c.load()
    c.settings.step = 'invalid_step'

    with pytest.raises(SystemExit) as e:
        c._validate_step()

    assert e.type is SystemExit
    assert e.value.code == 1


def test_validate_step_empty(c):
    c = Config()
    c.load()
    c.settings.step = ''

    with pytest.raises(SystemExit) as e:
        c._validate_step()

    assert e.type is SystemExit
    assert e.value.code == 1


def test_task_definitions_valid(c):
    c = Config()
    c.load()

    assert c.task_definitions == [TaskDefinition(name='task_1')]


def test_task_definitions_invalid_step(c):
    c = Config()
    c.load()
    c.settings.step = 'step_2'

    with pytest.raises(SystemExit) as e:
        c._init_task_definitions()

    assert e.type is SystemExit
    assert e.value.code == 1


def test_task_definitions_invalid_validation(c):
    c = Config()
    c.load()
    c._config_dict['steps'] = {'step_1': [{'name': True}]}

    with pytest.raises(SystemExit) as e:
        c._init_task_definitions()

    assert e.type is SystemExit
    assert e.value.code == 1
