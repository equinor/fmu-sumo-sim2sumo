# When ERT runs it makes changes to the files.
# This can cause issues for other tests if they expect certain files
# to exist etc. Tests that run ERT should therefore create their own
# temporary file structure, completely separate from other tests.
from pathlib import Path
from subprocess import PIPE, Popen


def write_ert_config_and_run(runpath):
    ert_config_path = "sim2sumo.ert"
    encoding = "utf-8"
    ert_full_config_path = runpath / ert_config_path
    print(f"Running with path {ert_full_config_path}")
    with open(ert_full_config_path, "w", encoding=encoding) as stream:
        stream.write(
            (
                "DEFINE <SUMO_ENV> dev\nNUM_REALIZATIONS 1\nMAX_SUBMIT"
                f" 1\nRUNPATH {runpath}\nFORWARD_MODEL SIM2SUMO"
            )
        )
    with Popen(
        ["ert", "test_run", str(ert_full_config_path)],
        stdout=PIPE,
        stderr=PIPE,
    ) as process:
        stdout, stderr = process.communicate()

    print(
        (
            "After ERT run these files were found at runpath:"
            f"{[item.name for item in list(Path(runpath).glob('*'))]}"
        )
    )
    if stdout:
        print("stdout:", stdout.decode(encoding), sep="\n")
    if stderr:
        print("stderr:", stderr.decode(encoding), sep="\n")
    try:
        error_content = Path(runpath / "ERROR").read_text(encoding=encoding)
    except FileNotFoundError:
        error_content = ""
    assert (
        not error_content
    ), f"ERROR file found with content:\n{error_content}"
    assert Path(
        runpath / "OK"
    ).is_file(), f"running {ert_full_config_path}, No OK file"


def test_sim2sumo_with_ert(
    ert_run_scratch_files, ert_run_case_uuid, sumo, monkeypatch
):
    monkeypatch.chdir(ert_run_scratch_files[0])
    real0 = ert_run_scratch_files[0]
    # ! This changes files in the current directory and deletes parameters.txt
    write_ert_config_and_run(real0)
    expected_exports = 88
    path = f"/objects('{ert_run_case_uuid}')/search"
    results = sumo.post(
        path,
        json={
            "query": {
                "bool": {
                    "must_not": [
                        {
                            "terms": {
                                "class.keyword": [
                                    "case",
                                    "iteration",
                                    "realization",
                                ]
                            }
                        }
                    ],
                }
            },
            "size": 0,
            "track_total_hits": True,
        },
    ).json()

    returned = results["hits"]["total"]["value"]
    assert (
        returned == expected_exports
    ), f"Supposed to upload {expected_exports}, but actual were {returned}"
