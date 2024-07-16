# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for NetCDF Utilities.
"""

import json
from django.test import TestCase
from datetime import datetime
import numpy as np
from django.contrib.gis.geos import (
    Point, GeometryCollection, Polygon, MultiPolygon
)
from unittest.mock import Mock, MagicMock, patch

from gap.utils.reader import (
    DatasetTimelineValue,
    DatasetReaderValue,
    LocationDatasetReaderValue,
    DatasetReaderInput,
    LocationInputType
)
from gap.utils.netcdf import (
    NetCDFProvider,
    daterange_inc,
    BaseNetCDFReader,
)
from gap.providers import (
    CBAMNetCDFReader,
    SalientNetCDFReader,
    get_reader_from_dataset
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory
)


class TestNetCDFProvider(TestCase):
    """Unit test for NetCDFProvider class."""

    def test_constants(self):
        """Test for correct constants."""
        self.assertEqual(NetCDFProvider.CBAM, 'CBAM')
        self.assertEqual(NetCDFProvider.SALIENT, 'Salient')


class TestDaterangeInc(TestCase):
    """Unit test for daterange_inc function."""

    def test_daterange(self):
        """Test for daterance_inc function."""
        start_date = datetime(2023, 1, 1)
        end_date = datetime(2023, 1, 3)
        expected_dates = [
            datetime(2023, 1, 1),
            datetime(2023, 1, 2),
            datetime(2023, 1, 3)
        ]
        result_dates = list(daterange_inc(start_date, end_date))
        self.assertEqual(result_dates, expected_dates)


class TestDatasetTimelineValue(TestCase):
    """Unit test for class DatasetTimelineValue."""

    def test_to_dict_with_datetime(self):
        """Test to_dict with python datetime object."""
        dt = datetime(2023, 7, 16, 12, 0, 0)
        values = {"temperature": 25}
        dtv = DatasetTimelineValue(dt, values)
        expected = {
            'datetime': '2023-07-16T12:00:00',
            'values': values
        }
        self.assertEqual(dtv.to_dict(), expected)

    def test_to_dict_with_np_datetime64(self):
        """Test to_dict with numpy datetime64 object."""
        dt = np.datetime64('2023-07-16T12:00:00')
        values = {"temperature": 25}
        dtv = DatasetTimelineValue(dt, values)
        expected = {
            'datetime': np.datetime_as_string(dt, unit='s', timezone='UTC'),
            'values': values
        }
        self.assertEqual(dtv.to_dict(), expected)

    def test_to_dict_with_none_datetime(self):
        """Test to_dict with empty datetime."""
        dtv = DatasetTimelineValue(None, {"temperature": 25})
        expected = {
            'datetime': '',
            'values': {"temperature": 25}
        }
        self.assertEqual(dtv.to_dict(), expected)


class TestDatasetReaderValue(TestCase):
    """Unit test for class DatasetReaderValue."""

    def test_to_dict_with_location(self):
        """Test to_dict with location."""
        location = Point(1, 1)
        dtv = DatasetTimelineValue(
            datetime(2023, 7, 16, 12, 0, 0), {"temperature": 25})
        drv = DatasetReaderValue(location, [dtv])
        expected = {
            'geometry': json.loads(location.json),
            'data': [dtv.to_dict()]
        }
        self.assertEqual(drv.to_dict(), expected)

    def test_to_dict_with_none_location(self):
        """Test to_dict with empty location."""
        drv = DatasetReaderValue(None, [])
        expected = {}
        self.assertEqual(drv.to_dict(), expected)


class TestLocationDatasetReaderValue(TestCase):
    """Unit test for LocationDatasetReaderValue class."""

    def test_to_dict(self):
        """Test to_dict method returrning dictionary."""
        location1 = Point(1, 1)
        location2 = Point(2, 2)
        dtv1 = DatasetTimelineValue(
            datetime(2023, 7, 16, 12, 0, 0), {"temperature": 25})
        dtv2 = DatasetTimelineValue(
            datetime(2023, 7, 16, 13, 0, 0), {"temperature": 26})
        results = {location1: [dtv1], location2: [dtv2]}
        ldrv = LocationDatasetReaderValue(results)
        expected = [
            DatasetReaderValue(location1, [dtv1]).to_dict(),
            DatasetReaderValue(location2, [dtv2]).to_dict()
        ]
        self.assertEqual(ldrv.to_dict(), expected)


class TestDatasetReaderInput(TestCase):
    """Unit test for DatasetReaderInput class."""

    def test_point(self):
        """Test get property point."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        self.assertEqual(dri.point, Point(1, 1, srid=4326))

    def test_polygon(self):
        """Test get property polygon."""
        polygon = MultiPolygon(Polygon(((0, 0), (1, 0), (1, 1), (0, 0))))
        geom_collection = GeometryCollection(polygon)
        dri = DatasetReaderInput(geom_collection, LocationInputType.POLYGON)
        self.assertEqual(dri.polygon, geom_collection)

    def test_points(self):
        """Test get property points."""
        points = [Point(1, 1), Point(2, 2)]
        geom_collection = GeometryCollection(*points)
        dri = DatasetReaderInput(
            geom_collection, LocationInputType.LIST_OF_POINT)
        self.assertEqual(
            dri.points, [Point(1, 1, srid=4326), Point(2, 2, srid=4326)])

    def test_from_point(self):
        """Test create object from point."""
        point = Point(1, 1, srid=4326)
        dri = DatasetReaderInput.from_point(point)
        self.assertEqual(dri.type, LocationInputType.POINT)
        self.assertEqual(dri.point, point)

    def test_from_bbox(self):
        """Test create object from bbox."""
        bbox_list = [1.0, 1.0, 2.0, 2.0]
        dri = DatasetReaderInput.from_bbox(bbox_list)
        self.assertEqual(dri.type, LocationInputType.BBOX)
        expected_points = [
            Point(x=bbox_list[0], y=bbox_list[1], srid=4326),
            Point(x=bbox_list[2], y=bbox_list[3], srid=4326)
        ]
        self.assertEqual(dri.points, expected_points)

    def test_invalid_point_access(self):
        """Test get property point with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.BBOX)
        with self.assertRaises(TypeError):
            _ = dri.point

    def test_invalid_polygon_access(self):
        """Test get property polygon with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        with self.assertRaises(TypeError):
            _ = dri.polygon

    def test_invalid_points_access(self):
        """Test get property points with invalid type."""
        geom_collection = GeometryCollection(Point(1, 1))
        dri = DatasetReaderInput(geom_collection, LocationInputType.POINT)
        with self.assertRaises(TypeError):
            _ = dri.points


class TestBaseNetCDFReader(TestCase):
    """Unit test for class BaseNetCDFReader."""

    def test_add_attribute(self):
        """Test adding a new attribute to Reader."""
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        self.assertEqual(len(reader.attributes), 0)
        reader.add_attribute(DatasetAttributeFactory.create())
        self.assertEqual(len(reader.attributes), 1)

    def test_get_attributes_metadata(self):
        """Test get attributes metadata dict."""
        attrib = DatasetAttributeFactory.create()
        attrib.attribute.variable_name = 'temperature'
        attrib.attribute.unit.name = 'C'
        attrib.attribute.name = 'Temperature'
        reader = BaseNetCDFReader(Mock(), [attrib], Mock(), Mock(), Mock())
        expected = {
            'temperature': {
                'units': 'C',
                'longname': 'Temperature'
            }
        }
        self.assertEqual(reader.get_attributes_metadata(), expected)

    def test_read_variables(self):
        """Test reading variables."""
        dataset = DatasetFactory.create(name=NetCDFProvider.CBAM)
        attribute = AttributeFactory.create(
            name='Temperature', variable_name='temperature')
        dataset_attr = DatasetAttributeFactory(
            dataset=dataset, attribute=attribute)
        reader = BaseNetCDFReader(
            dataset, [dataset_attr],
            DatasetReaderInput.from_point(Point(x=29.125, y=-2.215)),
            Mock(), Mock())
        xrArray = MagicMock()
        xrArray.sel.return_value = []
        xrDataset = MagicMock()
        xrDataset.__getitem__.return_value = xrArray
        result = reader.read_variables(xrDataset)
        self.assertEqual(result, [])

    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_client_kwargs')
    @patch('gap.utils.netcdf.NetCDFProvider.get_s3_variables')
    @patch('fsspec.filesystem')
    def test_setupNetCDFReader(
        self, mock_filesystem, mock_get_s3_vars, mock_get_s3_kwargs):
        """Test for setup NetCDFReader class."""
        mock_get_s3_kwargs.return_value = {
            'endpoint_url': 'test_endpoint'
        }
        mock_get_s3_vars.return_value = {
            'AWS_ACCESS_KEY_ID': 'test_key_id',
            'AWS_SECRET_ACCESS_KEY': 'test_key_secret',
        }
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.setup_netcdf_reader()
        mock_filesystem.assert_called_once_with(
            's3',
            key='test_key_id',
            secret='test_key_secret',
            client_kwargs=dict(endpoint_url='test_endpoint'))

    @patch('xarray.open_dataset')
    def test_open_dataset(self, mock_open_dataset):
        """Test for opening a dataset."""
        reader = BaseNetCDFReader(Mock(), [], Mock(), Mock(), Mock())
        reader.fs = Mock()
        reader.s3 = {
            'AWS_DIR_PREFIX': '',
            'AWS_BUCKET_NAME': 'test_bucket'
        }
        netcdf_file = Mock()
        netcdf_file.name = 'test_file.nc'
        reader.open_dataset(netcdf_file)
        mock_open_dataset.assert_called_once()

    def test_from_dataset(self):
        """Test for creating NetCDFReader from dataset."""
        dataset1 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.CBAM))
        reader = get_reader_from_dataset(dataset1)
        self.assertEqual(reader, CBAMNetCDFReader)
        dataset2 = DatasetFactory.create(
            provider=ProviderFactory(name=NetCDFProvider.SALIENT))
        reader = get_reader_from_dataset(dataset2)
        self.assertEqual(reader, SalientNetCDFReader)
        # invalid type
        dataset3 = DatasetFactory.create()
        with self.assertRaises(TypeError):
            get_reader_from_dataset(dataset3)
