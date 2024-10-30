# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Mixin for API Tracking
"""

import time
from redis import Redis
from django.core.cache import cache
from rest_framework.throttling import BaseThrottle

from gap_api.models.rate_limiter import APIRateLimiter


class RateLimitKey:
    """Key for available rate limit."""

    RATE_LIMIT_MINUTE_KEY = 1
    RATE_LIMIT_HOUR_KEY = 60
    RATE_LIMIT_DAY_KEY = 1440


class RateLimiter:
    """RateLimiter using sliding window counter."""

    def __init__(self, user_id, rate_limits):
        """Initialize rate limiter.

        :param user_id: User identifier.
        :param rate_limits: Dictionary with limit_duration_in_minutes:
            max_requests. E.g., {1: 100, 60: 1000, 1440: 10000}
            (100 requests per minute, 1000 per hour, 10,000 per day).
        """
        self.user_id = user_id
        self.rate_limits = rate_limits
        self.redis: Redis = cache._cache.get_client()
        self.exceeding_limits = []

    def _get_current_minute(self):
        """Get the current timestamp rounded to the nearest min."""
        return int(time.time() // 60)

    def _get_current_hour(self):
        """Get the current timestamp rounded to the nearest hour."""
        return int(time.time() // 3600)

    def _get_redis_key(self, granularity):
        """Get the Redis key for this user and time granularity."""
        if granularity == 'minute':
            return f"rate_limit:minute:{self.user_id}"
        elif granularity == 'hour':
            return f"rate_limit:hour:{self.user_id}"
        else:
            raise ValueError("Unsupported granularity")

    def _increment_request_count(self):
        """Increment the request count for the current minute and hour."""
        current_minute = self._get_current_minute()
        current_hour = self._get_current_hour()

        # Increment minute-level requests
        minute_key = self._get_redis_key('minute')
        self.redis.hincrby(minute_key, current_minute, 1)
        # 2 hours expiration for minute data
        self.redis.expire(minute_key, 2 * 60 * 60)

        # Increment hour-level requests (for daily limit)
        hour_key = self._get_redis_key('hour')
        self.redis.hincrby(hour_key, current_hour, 1)
        # 25 hours expiration for hour data
        self.redis.expire(hour_key, 25 * 60 * 60)

        # Clean up old minute and hour entries
        self._cleanup_old_entries()

    def _cleanup_old_entries(self):
        """Remove counters older than the longest rate limit window."""
        current_minute = self._get_current_minute()
        current_hour = self._get_current_hour()

        # Cleanup minute-level data (older than
        # the longest minute-based window)
        longest_minute_window = max(
            [duration for duration in self.rate_limits.keys() if
             duration < 60]
        )
        minute_cutoff = current_minute - longest_minute_window
        minute_key = self._get_redis_key('minute')
        minute_buckets = self.redis.hkeys(minute_key)
        for minute in minute_buckets:
            if int(minute) < minute_cutoff:
                self.redis.hdel(minute_key, minute)

        # Cleanup hour-level data (older than the longest hour-based window,
        # i.e., 24 hours)
        hour_cutoff = current_hour - 24
        hour_key = self._get_redis_key('hour')
        hour_buckets = self.redis.hkeys(hour_key)
        for hour in hour_buckets:
            if int(hour) < hour_cutoff:
                self.redis.hdel(hour_key, hour)

    def _get_request_count(self, duration_in_minutes):
        """Get the total request count for the last `duration_in_minutes`."""
        if duration_in_minutes < 60:
            # Minute-based rate limit
            # (for short durations like 1 minute or 1 hour)
            current_minute = self._get_current_minute()
            redis_key = self._get_redis_key('minute')
            total_count = 0
            for i in range(duration_in_minutes):
                minute = current_minute - i
                count = self.redis.hget(redis_key, minute)
                if count:
                    total_count += int(count)
            return total_count

        else:
            # Hour-based rate limit (for longer durations like a day)
            current_hour = self._get_current_hour()
            redis_key = self._get_redis_key('hour')
            total_count = 0
            for i in range(duration_in_minutes // 60):
                hour = current_hour - i
                count = self.redis.hget(redis_key, hour)
                if count:
                    total_count += int(count)
            return total_count

    def is_rate_limited(self):
        """Check if the user is rate-limited based on defined rate limits."""
        self.exceeding_limits = []
        for duration_in_minutes, max_requests in self.rate_limits.items():
            request_count = self._get_request_count(duration_in_minutes)
            if request_count >= max_requests:
                self.exceeding_limits.append(duration_in_minutes)

        if len(self.exceeding_limits) > 0:
            return True
        return False

    def is_request_allowed(self):
        """Check and increment the counter if request is allowed."""
        if self.is_rate_limited():
            return False

        self._increment_request_count()
        return True

    def get_waiting_time_in_seconds(self):
        """
        Estimate the waiting time (in seconds) until the rate limit is lifted.

        This is calculated as the time remaining until the next window starts.
        """
        current_time = time.time()
        waiting_times = []

        # check minute-level rate limit
        if any(duration < 60 for duration in self.exceeding_limits):
            next_minute_reset = (current_time // 60 + 1) * 60
            waiting_times.append(next_minute_reset - current_time)

        # check hour-level rate limit
        if RateLimitKey.RATE_LIMIT_HOUR_KEY in self.exceeding_limits:
            next_hour_reset = (current_time // 3600 + 1) * 3600
            waiting_times.append(next_hour_reset - current_time)

        # check day-level rate limit
        if RateLimitKey.RATE_LIMIT_DAY_KEY in self.exceeding_limits:
            next_day_reset = (current_time // 86400 + 1) * 86400
            waiting_times.append(next_day_reset - current_time)

        # Return the longest waiting time
        if waiting_times:
            return int(max(waiting_times))

        return None


class CounterSlidingWindowThrottle(BaseThrottle):
    """Custom throttle class using sliding window counter."""

    def _fetch_rate_limit(self, user):
        """Fetch rate limit for given user.

        if user does not have config, then it will use the global config.
        """
        config = APIRateLimiter.get_config(user)
        
        rate_limits = {}
        if config is None:
            return rate_limits

        if config['minute'] != -1:
            rate_limits[RateLimitKey.RATE_LIMIT_MINUTE_KEY] = config['minute']

        if config['hour'] != -1:
            rate_limits[RateLimitKey.RATE_LIMIT_HOUR_KEY] = config['hour']

        if config['day'] != -1:
            rate_limits[RateLimitKey.RATE_LIMIT_DAY_KEY] = config['day']

        return rate_limits

    def allow_request(self, request, view):
        """Check whether request is allowed."""
        rate_limits = self._fetch_rate_limit(request.user)

        # check if rate_limit is disabled
        # NOTE: the global config is 1k/min, 10k/hour, 100k/day from fixture
        if len(rate_limits) == 0:
            return True

        rate_limiter = RateLimiter(request.user.id, rate_limits)

        self.wait_time = None
        is_allowed = rate_limiter.is_request_allowed()
        if not is_allowed:
            self.wait_time = rate_limiter.get_waiting_time_in_seconds()

        return is_allowed

    def wait(self):
        """Return the waiting time in seconds."""
        return self.wait_time
