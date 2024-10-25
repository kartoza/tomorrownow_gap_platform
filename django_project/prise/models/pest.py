# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Pest prise models.
"""

from django.db import models
from django.utils.translation import gettext_lazy as _

from gap.models.pest import Pest
from prise.exceptions import PestVariableNameNotRecognized


class PrisePest(models.Model):
    """Pest specifically for Prise.

    This is for mapping the variable name to the pest model.
    """

    variable_name = models.CharField(
        max_length=256,
        help_text='Pest variable name that being used on CABI PRISE CSV.',
        unique=True
    )
    pest = models.ForeignKey(
        Pest, on_delete=models.CASCADE
    )

    class Meta:  # noqa
        db_table = 'prise_pest'
        verbose_name = _('Pest')

    @staticmethod
    def get_pest_by_variable_name(pest_variable_name) -> Pest:
        """Return Pest by variable name."""
        try:
            return PrisePest.objects.get(
                variable_name=pest_variable_name
            ).pest
        except PrisePest.DoesNotExist:
            raise PestVariableNameNotRecognized(pest_variable_name)
