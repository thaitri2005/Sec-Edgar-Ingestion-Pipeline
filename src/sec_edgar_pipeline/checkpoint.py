from __future__ import annotations

import logging

from sec_edgar_pipeline.logging_config import log_event
from sec_edgar_pipeline.metadata import MetadataRepository


def recover_run(
    repository: MetadataRepository,
    run_id: int,
    logger: logging.Logger,
) -> int:
    recovered = repository.recover_stale_running(run_id)
    if recovered:
        log_event(
            logger,
            logging.WARNING,
            "checkpoint_recovered",
            f"Recovered {recovered} stale running records",
            run_id=run_id,
            recovered_records=recovered,
        )
    return recovered
