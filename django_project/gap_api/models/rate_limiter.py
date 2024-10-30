# coding=utf-8
"""
Tomorrow Now GAP API.

.. note:: Models for Rate Limiter
"""

from django.db import models
from django.conf import settings
from django.core.cache import cache
from django.db.models.signals import post_save, pre_delete
from django.dispatch import receiver


class APIRateLimiter(models.Model):
    """Models that stores GAP API rate limiter."""

    CACHE_PREFIX_KEY = 'gap-api-ratelimit-'
    GLOBAL_CACHE_KEY = f'{CACHE_PREFIX_KEY}global'

    user = models.ForeignKey(
        settings.AUTH_USER_MODEL,
        on_delete=models.SET_NULL,
        null=True,
        blank=True
    )
    minute_limit = models.IntegerField()
    hour_limit = models.IntegerField()
    day_limit = models.IntegerField()

    @property
    def config_name(self):
        """Return config name."""
        if self.user:
            return self.user.username
        return 'global'

    @property
    def cache_key(self):
        """Return cache key for this config."""
        if self.user:
            return f'{APIRateLimiter.CACHE_PREFIX_KEY}{self.user.id}'
        return APIRateLimiter.GLOBAL_CACHE_KEY

    @property
    def cache_value(self):
        """Get dict cache value."""
        return {
            'minute': self.minute_limit,
            'hour': self.hour_limit,
            'day': self.day_limit
        }

    def set_cache(self):
        """Set rate limit to cache."""
        cache.set(
            self.cache_key,
            f'{self.minute_limit}:{self.hour_limit}:{self.day_limit}',
            timeout=None
        )

    def clear_cache(self):
        """Clear cache for this config."""
        cache.delete(self.cache_key)

    @staticmethod
    def parse_cache_value(cache_str: str):
        """Parse cache value."""
        values = cache_str.split(':')
        return {
            'minute': int(values[0]),
            'hour': int(values[1]),
            'day': int(values[2])
        }

    @staticmethod
    def get_global_config():
        """Get global config cache."""
        config_cache = cache.get(APIRateLimiter.GLOBAL_CACHE_KEY, None)
        if config_cache:
            return APIRateLimiter.parse_cache_value(config_cache)

        limit = APIRateLimiter.objects.filter(
            user=None
        ).first()
        if limit:
            # set to cache
            limit.set_cache()
            return limit.cache_value

        return None

    @staticmethod
    def get_config(user):
        """Return config for given user."""
        cache_key = f'{APIRateLimiter.CACHE_PREFIX_KEY}{user.id}'
        config_cache = cache.get(cache_key, None)

        if config_cache == 'global':
            # use global config
            pass
        elif config_cache is None:
            # find from table
            limit = APIRateLimiter.objects.filter(
                user=user
            ).first()

            if limit:
                # set to cache if found
                limit.set_cache()
                return limit.cache_value
            else:
                # set to use global config
                cache.set(cache_key, 'global')
        else:
            # parse config for the user
            return APIRateLimiter.parse_cache_value(config_cache)

        return APIRateLimiter.get_global_config()


@receiver(post_save, sender=APIRateLimiter)
def ratelimiter_post_create(
        sender, instance: APIRateLimiter, created, *args, **kwargs):
    """Clear cache after saving the object."""
    cache.delete(instance.cache_key)


@receiver(pre_delete, sender=APIRateLimiter)
def ratelimiter_pre_delete(
        sender, instance: APIRateLimiter, *args, **kwargs):
    """Clear cache before the model is deleted."""
    cache.delete(instance.cache_key)
