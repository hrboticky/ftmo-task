"""Part of the homework infrastructure. Don't change; see CONTRACT.md."""

import asyncio
import logging

from simulator.config import Config
from simulator.server import run


def main() -> None:
    config = Config()
    logging.basicConfig(level=config.log_level, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    asyncio.run(run(config))


if __name__ == "__main__":
    main()
