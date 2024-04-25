import os
import time
from pathlib import Path
import logging
from fmu.sumo import uploader

# Run the tests from the root dir
TEST_DIR = Path(__file__).parent / "../"
os.chdir(TEST_DIR)

ENV = "dev"

logger = logging.getLogger(__name__)
logger.setLevel(level="DEBUG")

class SumoConnection:
    def __init__(self, env, token=None):
        self.env = env
        self._connection = None
        self.token = token

    @property
    def connection(self):
        if self._connection is None:
            self._connection = uploader.SumoConnection(
                env=self.env, token=self.token
            )
        return self._connection

def test_case(token):
    """testing testing"""
    sumo_connection = uploader.SumoConnection(env=ENV, token=token)

    case_file = "tests/data/reek/share/metadata/rowh_case.yml"
    e = uploader.CaseOnDisk(
        case_metadata_path=case_file,
        sumo_connection=sumo_connection,
    )

    # Register the case
    e.register()
    time.sleep(1)

    # assert that the case is there now
    query = f"class:case AND fmu.case.uuid:{e.fmu_case_uuid}"
    search_results = sumo_connection.api.get(
        "/search", {"$query": query, "$size": 100}
    ).json()
    hits = search_results.get("hits").get("hits")
    logger.debug(search_results.get("hits"))
    assert len(hits) == 1
