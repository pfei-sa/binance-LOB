import json
from pathlib import Path
from typing import Dict, Any, List

from pydantic import BaseSettings


def json_config_settings_source(settings: BaseSettings) -> Dict[str, Any]:
    """
    A simple settings source that loads variables from a JSON file
    at the project's root.

    Here we happen to choose to use the `env_file_encoding` from Config
    when reading `config.json`
    """
    encoding = settings.__config__.env_file_encoding
    return json.loads(Path("config.json").read_text(encoding))


class Config(BaseSettings):
    api_key: str = ""
    api_secret: str = ""
    symbols: List[str]
    full_fetch_interval: int = 60 * 60
    full_fetch_limit: int = 1000
    stream_interval: int = 100
    log_to_console: bool = True
    dispatcher_buffer_size: int = 1000
    db_name: str = "archive"

    class Config:
        env_file_encoding = "utf-8"

        @classmethod
        def customise_sources(
            cls,
            init_settings,
            env_settings,
            file_secret_settings,
        ):
            return (
                init_settings,
                json_config_settings_source,
                env_settings,
                file_secret_settings,
            )


if __name__ == "__main__":
    print(Config())
