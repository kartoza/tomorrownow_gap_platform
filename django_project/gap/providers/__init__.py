# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from gap.models import Dataset
from gap.utils.netcdf import NetCDFProvider
from gap.providers.cbam import CBAMZarrReader, CBAMNetCDFReader  # noqa
from gap.providers.salient import SalientNetCDFReader, SalientZarrReader  # noqa
from gap.providers.tahmo import TahmoDatasetReader
from gap.providers.tio import (
    TomorrowIODatasetReader,
    PROVIDER_NAME as TIO_PROVIDER
)


def get_reader_from_dataset(dataset: Dataset):
    """Create a new Reader from given dataset.

    :param dataset: Dataset to be read
    :type dataset: Dataset
    :raises TypeError: if provider is neither CBAM or Salient
    :return: Reader Class Type
    :rtype: BaseDatasetReader
    """
    if dataset.provider.name == NetCDFProvider.CBAM:
        return CBAMZarrReader
    elif dataset.provider.name == NetCDFProvider.SALIENT:
        return SalientZarrReader
    elif dataset.provider.name == 'Tahmo':
        return TahmoDatasetReader
    elif dataset.provider.name == TIO_PROVIDER:
        return TomorrowIODatasetReader
    else:
        raise TypeError(
            f'Unsupported provider name: {dataset.provider.name}'
        )
