# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Config
"""

from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from core.models.common import Definition
from gap.models.common import Country


class DCASConfig(Definition):
    """Model that represents DCAS Configuration."""

    is_default = models.BooleanField(default=False)

    class Meta:  # noqa
        db_table = 'dcas_config'
        verbose_name = _('Config')


class DCASConfigCountry(models.Model):
    """Model that represents config for specific country."""

    config = models.ForeignKey(
        DCASConfig, on_delete=models.CASCADE
    )
    country = models.ForeignKey(
        Country, on_delete=models.CASCADE
    )

    class Meta:  # noqa
        db_table = 'dcas_config_country'
        verbose_name = _('Country Config')
