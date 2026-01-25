def test_initialisation_is_valid(init_result):
    assert init_result.ok, (
        f"postgres db failed to initialise after timeout! Logs: {init_result.logs}"
    )
