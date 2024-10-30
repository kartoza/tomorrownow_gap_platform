# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Mixin for API Tracking
"""

import time
from redis import Redis
from django.core.cache import cache
from rest_framework.throttling import BaseThrottle


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
        for duration_in_minutes, max_requests in self.rate_limits.items():
            request_count = self._get_request_count(duration_in_minutes)
            if request_count >= max_requests:
                return True
        return False


class CounterSlidingWindowThrottle(BaseThrottle):
    """Custom throttle class using sliding window counter."""

    def allow_request(self, request, view):
        """Check whether request is allowed."""
        rate_limits = {
            1: 100,    # 100 requests per minute
            60: 1000,  # 1000 requests per hour
            1440: 10000  # 10,000 requests per day
        }

        rate_limiter = RateLimiter(request.user.id, rate_limits)

        if rate_limiter.is_rate_limited():
            return False

        rate_limiter._increment_request_count()
        return True
