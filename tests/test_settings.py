import os
from pathlib import Path

import pytest

from embyx_web.settings import Settings


def test_settings_use_explicit_non_secret_runtime_paths(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    monkeypatch.setenv('EMBYX_WEB_DATABASE_PATH', str(tmp_path / 'state.sqlite3'))
    monkeypatch.setenv('EMBYX_WEB_ACTOR_ROOT', str(tmp_path / 'actor'))
    monkeypatch.setenv(
        'EMBYX_WEB_ADDITIONAL_ROOTS',
        os.pathsep.join((str(tmp_path / 'additional-1'), str(tmp_path / 'additional-2'))),
    )
    monkeypatch.setenv('EMBYX_WEB_MOVE_IN_ROOT', str(tmp_path / 'move-in'))
    monkeypatch.setenv('EMBYX_WEB_RUNTIME_ROOT', str(tmp_path / 'runtime'))
    monkeypatch.setenv('EMBYX_WEB_MOVE_IN_BY_BRAND', 'true')
    monkeypatch.setenv('EMBYX_WEB_RSSHUB_URL', 'http://rsshub.rss.svc.cluster.local/')
    monkeypatch.setenv('EMBYX_WEB_FRESHRSS_URL', 'https://rss.s117.me/')

    settings = Settings.from_env()

    assert settings.database_path == tmp_path / 'state.sqlite3'
    assert settings.actor_brand_path == tmp_path / 'actor'
    assert settings.additional_brand_paths == (tmp_path / 'additional-1', tmp_path / 'additional-2')
    assert settings.move_in_path == tmp_path / 'move-in'
    assert settings.embyx_runtime_path == tmp_path / 'runtime'
    assert settings.move_in_by_brand is True
    assert settings.rsshub_url == 'http://rsshub.rss.svc.cluster.local'
    assert settings.freshrss_url == 'https://rss.s117.me'


def test_rsshub_defaults_to_cluster_service_and_empty_value_disables_it(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('EMBYX_WEB_RSSHUB_URL', raising=False)
    assert Settings.from_env().rsshub_url == 'http://rsshub.rss.svc.cluster.local'

    monkeypatch.setenv('EMBYX_WEB_RSSHUB_URL', '')
    assert Settings.from_env().rsshub_url is None


@pytest.mark.parametrize(
    ('name', 'value'),
    [
        ('EMBYX_WEB_RSSHUB_URL', 'ftp://rsshub.example'),
        ('EMBYX_WEB_RSSHUB_URL', 'http://user:password@rsshub.example'),
        ('EMBYX_WEB_FRESHRSS_URL', 'https://rss.example/?token=secret'),
    ],
)
def test_feed_integration_urls_reject_unsafe_bases(
    monkeypatch: pytest.MonkeyPatch,
    name: str,
    value: str,
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        Settings.from_env()


def test_non_loopback_binding_requires_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_WEB_HOST', '192.0.2.1')
    monkeypatch.delenv('EMBYX_WEB_API_TOKEN', raising=False)

    with pytest.raises(ValueError, match='API_TOKEN'):
        Settings.from_env()


def test_non_loopback_binding_accepts_token(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_WEB_HOST', '192.0.2.1')
    monkeypatch.setenv('EMBYX_WEB_API_TOKEN', 'configured-at-runtime')
    monkeypatch.setenv('EMBYX_WEB_TLS_TERMINATED', 'true')

    assert Settings.from_env().host == '192.0.2.1'


def test_non_loopback_binding_requires_tls_termination(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_WEB_HOST', '192.0.2.1')
    monkeypatch.setenv('EMBYX_WEB_API_TOKEN', 'configured-at-runtime')
    monkeypatch.delenv('EMBYX_WEB_TLS_TERMINATED', raising=False)

    with pytest.raises(ValueError, match='TLS_TERMINATED'):
        Settings.from_env()
