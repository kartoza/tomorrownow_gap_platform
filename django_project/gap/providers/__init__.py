# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Helper for reading NetCDF File
"""

from gap.models import Dataset
from gap.utils.netcdf import NetCDFProvider
from gap.providers.cbam import CBAMNetCDFReader
from gap.providers.salient import SalientNetCDFReader
from gap.providers.tahmo import TahmoDatasetReader


def get_reader_from_dataset(dataset: Dataset):
    """Create a new Reader from given dataset.

    :param dataset: Dataset to be read
    :type dataset: Dataset
    :raises TypeError: if provider is neither CBAM or Salient
    :return: Reader Class Type
    :rtype: CBAMNetCDFReader|SalientNetCDFReader
    """
    if dataset.provider.name == NetCDFProvider.CBAM:
        return CBAMNetCDFReader
    elif dataset.provider.name == NetCDFProvider.SALIENT:
        return SalientNetCDFReader
    elif dataset.provider.name == 'Tahmo':
        return TahmoDatasetReader
    else:
        raise TypeError(
            f'Unsupported provider name: {dataset.provider.name}'
        )
