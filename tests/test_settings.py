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
    monkeypatch.setenv('EMBYX_WEB_APPLY_ENABLED', 'true')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_STRM_MOUNT_PREFIX', '/mounted-cloud')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_SOURCE_ROOTS', '/cloud/library/additional-1:/cloud/library/additional-2')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_MOVE_IN_ROOT', '/cloud/library/destination')
    monkeypatch.setenv('EMBYX_WEB_RSSHUB_URL', 'http://rsshub.internal.test/')
    monkeypatch.setenv('EMBYX_WEB_FRESHRSS_URL', 'https://freshrss.example.test/')
    monkeypatch.setenv('EMBYX_WEB_FRESHRSS_RSSHUB_URL', 'https://rsshub.example.test/')

    settings = Settings.from_env()

    assert settings.database_path == tmp_path / 'state.sqlite3'
    assert settings.actor_brand_path == tmp_path / 'actor'
    assert settings.additional_brand_paths == (tmp_path / 'additional-1', tmp_path / 'additional-2')
    assert settings.move_in_path == tmp_path / 'move-in'
    assert settings.embyx_runtime_path == tmp_path / 'runtime'
    assert settings.move_in_by_brand is True
    assert settings.apply_enabled is True
    assert settings.cloud_move_paths is not None
    assert tuple(map(str, settings.cloud_move_paths.source_api_roots)) == (
        '/cloud/library/additional-1',
        '/cloud/library/additional-2',
    )
    assert settings.rsshub_url == 'http://rsshub.internal.test'
    assert settings.freshrss_url == 'https://freshrss.example.test'
    assert settings.freshrss_rsshub_url == 'https://rsshub.example.test'


def test_feed_integration_urls_default_to_disabled_and_empty_values_disable_them(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    names = ('EMBYX_WEB_RSSHUB_URL', 'EMBYX_WEB_FRESHRSS_URL', 'EMBYX_WEB_FRESHRSS_RSSHUB_URL')
    for name in names:
        monkeypatch.delenv(name, raising=False)
    settings = Settings.from_env()
    assert settings.rsshub_url is None
    assert settings.freshrss_url is None
    assert settings.freshrss_rsshub_url is None

    for name in names:
        monkeypatch.setenv(name, '')
    settings = Settings.from_env()
    assert settings.rsshub_url is None
    assert settings.freshrss_url is None
    assert settings.freshrss_rsshub_url is None


def test_apply_defaults_to_disabled(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv('EMBYX_WEB_APPLY_ENABLED', raising=False)

    assert Settings.from_env().apply_enabled is False


def test_apply_cannot_enable_legacy_local_file_moves(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_WEB_APPLY_ENABLED', 'true')

    with pytest.raises(ValueError, match='requires the CloudDrive'):
        Settings.from_env()


def test_cloud_roots_must_be_complete_and_match_additional_roots(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv('EMBYX_WEB_ADDITIONAL_ROOTS', '/mapping/a:/mapping/b')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_STRM_MOUNT_PREFIX', '/mounted-cloud')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_SOURCE_ROOTS', '/cloud/library/a')
    monkeypatch.setenv('EMBYX_WEB_CLOUD_MOVE_IN_ROOT', '/cloud/library/destination')

    with pytest.raises(ValueError, match='one-for-one'):
        Settings.from_env()


@pytest.mark.parametrize(
    ('name', 'value'),
    [
        ('EMBYX_WEB_RSSHUB_URL', 'ftp://rsshub.example'),
        ('EMBYX_WEB_RSSHUB_URL', 'http://user:password@rsshub.example'),
        ('EMBYX_WEB_FRESHRSS_URL', 'https://rss.example/?token=secret'),
        ('EMBYX_WEB_FRESHRSS_RSSHUB_URL', 'file:///var/lib/rsshub'),
        ('EMBYX_WEB_RSSHUB_URL', 'https://rsshub.example.test:not-a-port'),
        ('EMBYX_WEB_FRESHRSS_URL', 'https://freshrss.example.test:70000'),
        ('EMBYX_WEB_FRESHRSS_RSSHUB_URL', 'https://rsshub.example.test:0'),
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
