# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for User API.
"""

from mock import patch
from django.test import TestCase, override_settings
from django.core.cache import cache
from fakeredis import FakeConnection

from gap_api.models.rate_limiter import APIRateLimiter
from gap_api.mixins.rate_limiter import RateLimiter
from gap_api.factories import APIRateLimiterFactory


@override_settings(
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.redis.RedisCache',
            'LOCATION': [
                'redis://127.0.0.1:6379',
            ],
            'OPTIONS': {
                'connection_class': FakeConnection
            }
        }
    }
)
class TestRateLimiter(TestCase):
    """Unit test for RateLimiter class."""

    def setUp(self):
        """Set test class."""
        self.redis_client = cache._cache.get_client()

    def test_rate_limiter_allows_request(self):
        """Test requests are allowed."""
        # Set up rate limiter with small rate limits for testing
        rate_limiter = RateLimiter(
            user_id="user123", rate_limits={1: 5, 60: 100, 1440: 1000})

        # Send 5 requests, should all pass
        for _ in range(5):
            can_access = rate_limiter.is_request_allowed()
            self.assertTrue(can_access)

    def test_rate_limiter_blocks_after_minute_limit(self):
        """Test blocked after minute limit is exceeded."""
        # Set up rate limiter with small rate limits for testing
        rate_limiter = RateLimiter(
            user_id="user124", rate_limits={1: 3, 60: 100, 1440: 1000})

        # Send 3 requests, all should pass
        for _ in range(3):
            can_access = rate_limiter.is_request_allowed()
            self.assertTrue(can_access)

        # Send 1 more request, should be rate limited
        can_access = rate_limiter.is_request_allowed()
        self.assertFalse(can_access)

    def test_rate_limiter_resets_after_time(self):
        """Test rate limit is reset."""
        # Set up rate limiter with a 1-minute limit of 3 requests
        rate_limiter = RateLimiter(user_id="user125", rate_limits={1: 3})

        # Send 3 requests, all should pass
        for _ in range(3):
            can_access = rate_limiter.is_request_allowed()
            self.assertTrue(can_access)

        # Directly modify Redis to simulate time passing
        # (clear the minute bucket)
        current_minute = rate_limiter._get_current_minute()
        minute_key = rate_limiter._get_redis_key('minute')
        self.redis_client.hdel(minute_key, current_minute)

        # Send 1 more request, it should now pass
        can_access = rate_limiter.is_request_allowed()
        self.assertTrue(can_access)

    def test_rate_limiter_blocks_after_hour_limit(self):
        """Test blocked after hour limit is exceeded."""
        # Set up rate limiter with a 60-minute (1 hour) limit of 10 requests
        rate_limiter = RateLimiter(user_id="user126", rate_limits={60: 10})

        # Manually set Redis data to simulate 10 requests in the past hour
        current_hour = rate_limiter._get_current_hour()
        hour_key = rate_limiter._get_redis_key('hour')
        self.redis_client.hset(hour_key, current_hour, 10)

        # Send 1 more request, it should now be rate limited
        can_access = rate_limiter.is_request_allowed()
        self.assertFalse(can_access)

    def test_rate_limiter_blocks_after_day_limit(self):
        """Test blocked after day limit is exceeded."""
        # Set up rate limiter with a 1440-minute (1 day) limit of 50 requests
        rate_limiter = RateLimiter(user_id="user127", rate_limits={1440: 50})

        # Manually set Redis data to simulate 50 requests in the past 24 hours
        current_hour = rate_limiter._get_current_hour()
        hour_key = rate_limiter._get_redis_key('hour')
        for i in range(24):  # Simulate requests for the past 24 hours
            self.redis_client.hset(hour_key, current_hour - i, 5)

        # Send 1 more request, it should now be rate limited
        can_access = rate_limiter.is_request_allowed()
        self.assertFalse(can_access)

    def test_cleanup_old_entries(self):
        """Test cleanup old entries."""
        # Set up rate limiter with a 1-minute and 60-minute limit for testing
        rate_limiter = RateLimiter(
            user_id="user128", rate_limits={1: 5, 60: 100})

        # Manually set Redis data to simulate old minute and hour entries
        current_minute = rate_limiter._get_current_minute()
        current_hour = rate_limiter._get_current_hour()

        # Add old minute data beyond the longest minute-based window
        # (assume limit is 1 minute)
        minute_key = rate_limiter._get_redis_key('minute')
        # Older than 1 minute window
        self.redis_client.hset(minute_key, current_minute - 2, 10)

        # Add old hour data beyond the longest hour-based window
        # (assume 24 hours)
        hour_key = rate_limiter._get_redis_key('hour')
        # Older than 24-hour window
        self.redis_client.hset(hour_key, current_hour - 25, 10)

        # Perform a request to trigger cleanup
        rate_limiter.is_request_allowed()

        # Verify old minute data is cleaned up
        self.assertFalse(
            self.redis_client.hexists(minute_key, current_minute - 2))

        # Verify old hour data is cleaned up
        self.assertFalse(
            self.redis_client.hexists(hour_key, current_hour - 25))

    @patch('time.time', return_value=1730319778)
    def test_get_waiting_time_in_seconds_for_minute_limit(self, mock_time):
        """Test get waiting time for minute limit."""
        rate_limiter = RateLimiter(
            user_id="user129", rate_limits={1: 5, 60: 10, 1440: 100})
        rate_limiter.exceeding_limits = [1]
        wait_time = rate_limiter.get_waiting_time_in_seconds()
        self.assertTrue(0 < wait_time <= 60)
        mock_time.assert_called_once()

    @patch('time.time', return_value=1730319778)
    def test_get_waiting_time_in_seconds_for_hour_limit(self, mock_time):
        """Test get waiting time for hour limit."""
        rate_limiter = RateLimiter(
            user_id="user129", rate_limits={1: 5, 60: 10, 1440: 100})
        rate_limiter.exceeding_limits = [60]
        wait_time = rate_limiter.get_waiting_time_in_seconds()
        self.assertTrue(0 < wait_time <= 3600)
        mock_time.assert_called_once()

    @patch('time.time', return_value=1730319778)
    def test_get_waiting_time_in_seconds_for_day_limit(self, mock_time):
        """Test get waiting time for day limit."""
        rate_limiter = RateLimiter(
            user_id="user129", rate_limits={1: 5, 60: 10, 1440: 100})
        rate_limiter.exceeding_limits = [1440]
        wait_time = rate_limiter.get_waiting_time_in_seconds()
        self.assertTrue(0 < wait_time <= 86400)
        mock_time.assert_called_once()

    @patch('time.time', return_value=1730319778)
    def test_get_waiting_time_in_seconds_under_limit(self, mock_time):
        """Test get waiting time when under limit."""
        rate_limiter = RateLimiter(
            user_id="user129", rate_limits={1: 5, 60: 10, 1440: 100})
        wait_time = rate_limiter.get_waiting_time_in_seconds()
        self.assertIsNone(wait_time)
        mock_time.assert_called_once()

    @patch('time.time', return_value=1730319778)
    def test_get_waiting_time_in_seconds_multiple_limit(self, mock_time):
        """Test get waiting time when multiple limits are exceeded."""
        rate_limiter = RateLimiter(
            user_id="user129", rate_limits={1: 5, 60: 10, 1440: 100})
        rate_limiter.exceeding_limits = [1, 60]
        wait_time = rate_limiter.get_waiting_time_in_seconds()
        self.assertTrue(0 < wait_time <= 3600)
        mock_time.assert_called_once()


@override_settings(
    CACHES={
        'default': {
            'BACKEND': 'django.core.cache.backends.redis.RedisCache',
            'LOCATION': [
                'redis://127.0.0.1:6379',
            ],
            'OPTIONS': {
                'connection_class': FakeConnection
            }
        }
    }
)
class TestAPIRateLimiterModel(TestCase):
    """Unit test for APIRateLimiter model."""

    def setUp(self):
        """Set the test class."""
        self.global_rate_limiter = APIRateLimiterFactory.create(
            user=None,
            minute_limit=10,
            hour_limit=100,
            day_limit=1000,
        )
        self.user_rate_limiter = APIRateLimiterFactory.create(
            minute_limit=5,
            hour_limit=50,
            day_limit=500,
        )

    def test_set_cache_for_user(self):
        """Test setting cache for a specific user."""
        # Set cache for user-specific config
        self.user_rate_limiter.set_cache()
        cached_value = cache.get(self.user_rate_limiter.cache_key)
        self.assertEqual(cached_value, '5:50:500')

    def test_set_cache_for_global(self):
        """Test setting cache for global config."""
        # Set cache for global config
        self.global_rate_limiter.set_cache()
        cached_value = cache.get(self.global_rate_limiter.cache_key)
        self.assertEqual(cached_value, '10:100:1000')

    def test_clear_cache_for_user(self):
        """Test clearing cache for a specific user."""
        self.user_rate_limiter.set_cache()
        self.user_rate_limiter.clear_cache()
        cached_value = cache.get(self.user_rate_limiter.cache_key)
        self.assertIsNone(cached_value)

    def test_get_config_for_user(self):
        """Test retrieving config for a specific user from cache."""
        self.user_rate_limiter.set_cache()
        config = APIRateLimiter.get_config(self.user_rate_limiter.user)
        self.assertEqual(config['minute'], 5)
        self.assertEqual(config['hour'], 50)
        self.assertEqual(config['day'], 500)

    def test_get_global_config(self):
        """Test retrieving global config from cache."""
        self.global_rate_limiter.set_cache()
        config = APIRateLimiter.get_global_config()
        self.assertEqual(config['minute'], 10)
        self.assertEqual(config['hour'], 100)
        self.assertEqual(config['day'], 1000)

    def test_get_global_config_when_not_cached(self):
        """Test retrieving global config when it is not cached."""
        # First clear cache to simulate no cache scenario
        cache.delete(APIRateLimiter.GLOBAL_CACHE_KEY)

        # Ensure the global rate limiter is fetched and set in the cache
        config = APIRateLimiter.get_global_config()
        self.assertEqual(config['minute'], 10)
        self.assertEqual(config['hour'], 100)
        self.assertEqual(config['day'], 1000)
        cached_value = cache.get(APIRateLimiter.GLOBAL_CACHE_KEY)
        self.assertEqual(cached_value, '10:100:1000')
