# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Pest Models
"""

from django.contrib.gis.db import models
from django.utils.translation import gettext_lazy as _

from core.models.common import Definition


class TaxonomicRank:
    """Taxonomic rank for Pest."""

    DOMAIN = 'domain'
    KINGDOM = 'kingdom'
    PHYLUM = 'phylum'
    DIVISION = 'division'
    CLASS = 'class'
    ORDER = 'order'
    FAMILY = 'family'
    GENUS = 'genus'
    SPECIES = 'species'


class Pest(Definition):
    """Model representing pest."""

    scientific_name = models.CharField(
        max_length=512,
        null=True, blank=True
    )
    taxonomic_rank = models.CharField(
        null=True, blank=True,
        choices=(
            (TaxonomicRank.DOMAIN, _(TaxonomicRank.DOMAIN)),
            (TaxonomicRank.KINGDOM, _(TaxonomicRank.KINGDOM)),
            (TaxonomicRank.PHYLUM, _(TaxonomicRank.PHYLUM)),
            (TaxonomicRank.DIVISION, _(TaxonomicRank.DIVISION)),
            (TaxonomicRank.CLASS, _(TaxonomicRank.CLASS)),
            (TaxonomicRank.ORDER, _(TaxonomicRank.ORDER)),
            (TaxonomicRank.FAMILY, _(TaxonomicRank.FAMILY)),
            (TaxonomicRank.GENUS, _(TaxonomicRank.GENUS)),
            (TaxonomicRank.SPECIES, _(TaxonomicRank.SPECIES)),
        ),
        max_length=512
    )
    link = models.URLField(
        null=True, blank=True
    )
    short_name = models.CharField(
        max_length=20,
        null=True, blank=True
    )
