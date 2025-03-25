import pytest

from pragmatiq.metrics.monitor import PragmatiQConfig


def test_pragmatiq_config_init_success() -> None:
    """Test passing case: Default config initialization."""
    config = PragmatiQConfig()
    assert config.service_name == "pragmatiq"
    assert config.prometheus_port == 9090
    assert config.jaeger_auth is None


def test_pragmatiq_config_invalid_auth_fail() -> None:
    """Test failing case: Invalid auth tuple length."""
    with pytest.raises(expected_exception=ValueError):
        PragmatiQConfig(prometheus_auth=("user",))  # type: ignore[awaitable]


def test_pragmatiq_config_post_init_type_check_success() -> None:
    """Test passing case: All types are correct."""
    config = PragmatiQConfig(
        cron_check_interval=10,
        redis_host="127.0.0.1",
        jaeger_auth=("user", "pass"),
    )
    assert config.cron_check_interval == 10
    assert config.redis_host == "127.0.0.1"
    assert config.jaeger_auth == ("user", "pass")


def test_pragmatiq_config_post_init_type_check_fail() -> None:
    """Test failing case: Wrong type for attribute."""
    with pytest.raises(
        expected_exception=ValueError,
        match="must be of type",
    ):
        PragmatiQConfig(cron_check_interval="not_an_int")  # type: ignore[awaitable]


def test_pragmatiq_config_iter_success() -> None:
    """Test passing case: Config is iterable and yields all attributes."""
    config = PragmatiQConfig(service_name="iter_test")
    attrs = dict(config)
    assert attrs["service_name"] == "iter_test"
    assert len(attrs) == len(PragmatiQConfig.__annotations__)


def test_pragmatiq_config_invalid_auth_type_fail() -> None:
    """Test failing case: Auth tuple with non-string elements."""
    with pytest.raises(
        expected_exception=ValueError,
        match="must be a tuple containing exactly two strings",
    ):
        PragmatiQConfig(jaeger_auth=(123, "pass"))  # type: ignore[awaitable]


def test_pragmatiq_config_default_values_success() -> None:
    """Test passing case: Verify default values not explicitly tested before."""
    config = PragmatiQConfig()
    assert config.redis_host == "localhost"
    assert config.redis_port == 6379
    assert config.result_ttl == 3600
    assert config.process_pool_size == 4


def test_pragmatiq_config_negative_value_fail() -> None:
    """Test failing case: Negative value for a field that should be positive."""
    with pytest.raises(
        expected_exception=ValueError,
        match="must be a positive integer",
    ):  # Indirectly caught by type check
        PragmatiQConfig(cron_check_interval=-5)  # Should be positive int


@pytest.mark.asyncio
async def test_pragmatiq_config_validation_auth_tuple() -> None:
    """Test that PragmatiQConfig raises ValueError for invalid auth tuple length or type."""
    with pytest.raises(expected_exception=ValueError) as excinfo:
        PragmatiQConfig(prometheus_auth=("user",))  # type: ignore[awaitable]
    assert "must be a tuple containing exactly two strings" in str(excinfo.value)

    with pytest.raises(expected_exception=ValueError) as excinfo:
        PragmatiQConfig(jaeger_auth=(1, 2))  # type: ignore[awaitable]
    assert "must be a tuple containing exactly two strings" in str(excinfo.value)


@pytest.mark.asyncio
async def test_pragmatiq_config_validation_type() -> None:
    """Test that PragmatiQConfig raises ValueError for incorrect attribute type."""
    with pytest.raises(expected_exception=ValueError) as excinfo:
        PragmatiQConfig(redis_port="6379")  # type: ignore[awaitable]
    assert "must be of type '<class 'int'>'" in str(excinfo.value)
