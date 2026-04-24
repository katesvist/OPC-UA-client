from __future__ import annotations

import uvicorn

from src.bootstrap import create_app
from src.config.settings import load_settings

app = create_app()


def main() -> None:
    config = load_settings()
    uvicorn.run(
        app,
        host=config.api.host,
        port=config.api.port,
    )


if __name__ == "__main__":
    main()
