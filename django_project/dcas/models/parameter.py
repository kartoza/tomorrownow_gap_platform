# coding=utf-8
"""
Tomorrow Now GAP DCAS.

.. note:: Models for DCAS Parameter
"""

from django.utils.translation import gettext_lazy as _

from core.models.common import Definition


class DCASParameter(Definition):
    """Model that represent parameter in DCAS."""

    class Meta:  # noqa
        db_table = 'dcas_parameter'
        verbose_name = _('Parameter')
