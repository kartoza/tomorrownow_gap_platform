# coding=utf-8
"""
Tomorrow Now GAP.

.. note:: Unit tests for Tahmo Reader.
"""

from unittest.mock import patch, MagicMock
import duckdb

from django.test import TestCase
from datetime import datetime
from django.contrib.gis.geos import (
    Point, MultiPoint, MultiPolygon, Polygon,
    GEOSGeometry
)

from gap.providers import (
    ObservationDatasetReader
)
from gap.providers.observation import (
    ObservationReaderValue, ObservationParquetReaderValue,
    ObservationParquetReader
)
from gap.factories import (
    ProviderFactory,
    DatasetFactory,
    DatasetAttributeFactory,
    AttributeFactory,
    StationFactory,
    MeasurementFactory,
    StationTypeFactory
)
from gap.utils.reader import (
    DatasetReaderInput,
    LocationInputType
)


class TestObservationReader(TestCase):
    """Unit test for ObservationDatasetReader class."""

    def setUp(self):
        """Set test for ObservationDatasetReader."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name='Tahmo'))
        self.attribute = AttributeFactory.create(
            name='Surface air temperature',
            variable_name='surface_air_temperature')
        self.dataset_attr = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute,
            source='surface_air_temperature'
        )
        p = Point(x=26.97, y=-12.56, srid=4326)
        self.station = StationFactory.create(
            geometry=p,
            provider=self.dataset.provider
        )
        self.location_input = DatasetReaderInput.from_point(p)
        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 1, 31)
        self.reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], self.location_input,
            self.start_date, self.end_date
        )

    def test_find_nearest_station_empty(self):
        """Test find nearest station empty."""
        new_provider = ProviderFactory(name='test_provider')
        self.station.provider = new_provider
        self.station.save()

        self.reader.read_historical_data(self.start_date, self.end_date)
        data_value = self.reader.get_data_values()
        self.assertTrue(data_value.is_empty())

    def test_find_nearest_station_by_point(self):
        """Test find nearest station from single point."""
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=self.start_date,
            value=120
        )
        result = self.reader._find_nearest_station_by_point()
        self.assertEqual(result, [self.station])

    def test_find_nearest_station_by_bbox(self):
        """Test find nearest station from bbox."""
        self.reader.location_input = DatasetReaderInput.from_bbox(
            [-180, -90, 180, 90])
        result = self.reader._find_nearest_station_by_bbox()
        self.assertEqual(list(result), [self.station])

    def test_find_nearest_station_by_polygon(self):
        """Test find nearest station from polygon."""
        self.reader.location_input.type = LocationInputType.POLYGON
        self.reader.location_input.geom_collection = MultiPolygon(
            Polygon.from_bbox((-180, -90, 180, 90)))
        result = self.reader._find_nearest_station_by_polygon()
        self.assertEqual(list(result), [self.station])

    def test_find_nearest_station_by_points(self):
        """Test find nearest station from list of point."""
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=self.start_date,
            value=120
        )
        self.reader.location_input.type = LocationInputType.LIST_OF_POINT
        self.reader.location_input.geom_collection = MultiPoint(
            [Point(0, 0), self.station.geometry])
        result = self.reader._find_nearest_station_by_points()
        self.assertEqual(list(result), [self.station])

    def test_read_historical_data(self):
        """Test for reading historical data from Tahmo."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt,
            value=100
        )
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], DatasetReaderInput.from_point(
                self.station.geometry
            ), dt, dt)
        reader.read_historical_data(dt, dt)
        data_value = reader.get_data_values()
        results = data_value.to_json()
        self.assertEqual(len(results['data']), 1)
        self.assertEqual(
            results['data'][0]['values']['surface_air_temperature'],
            100)

    def test_read_historical_data_empty(self):
        """Test for reading empty historical data from Tahmo."""
        dt = datetime(2019, 11, 1, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt,
            value=100
        )
        points = DatasetReaderInput(MultiPoint(
            [Point(0, 0), Point(1, 1)]), LocationInputType.LIST_OF_POINT)
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], points, dt, dt)
        reader.read_historical_data(
            datetime(2010, 11, 1, 0, 0, 0),
            datetime(2010, 11, 1, 0, 0, 0))
        data_value = reader.get_data_values()
        self.assertEqual(data_value.count(), 0)

    def test_read_historical_data_by_point(self):
        """Test read by stations that has same point for different dataset."""
        other_dataset = DatasetFactory.create(
            provider=self.dataset.provider)
        other_attr = DatasetAttributeFactory.create(
            dataset=other_dataset,
            attribute=self.attribute,
            source='test_attr'
        )
        other_station = StationFactory.create(
            geometry=self.station.geometry,
            provider=self.dataset.provider,
            station_type=StationTypeFactory.create(name='other')
        )

        # create measurement
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        measurement = MeasurementFactory.create(
            station=other_station,
            dataset_attribute=other_attr,
            date_time=dt1,
            value=876
        )

        # create reader
        location_input = DatasetReaderInput.from_point(other_station.geometry)
        reader = ObservationDatasetReader(
            other_dataset, [other_attr], location_input,
            dt1, dt2
        )
        reader.read_historical_data(dt1, dt2)
        data_value = reader.get_data_values()
        # should return above measurement
        self.assertEqual(len(data_value._val), 1)
        dict_value = data_value.to_json()
        self.assertEqual(
            dict_value['data'][0]['values']['surface_air_temperature'],
            measurement.value
        )

    def test_read_historical_data_multiple_locations(self):
        """Test for reading historical data from multiple locations."""
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=100
        )
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt2,
            value=200
        )
        p = Point(x=28.97, y=-10.56, srid=4326)
        station2 = StationFactory.create(
            geometry=p,
            provider=self.dataset.provider
        )
        MeasurementFactory.create(
            station=station2,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=300
        )
        location_input = DatasetReaderInput(
            MultiPoint([self.station.geometry, p]),
            LocationInputType.LIST_OF_POINT)
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], location_input, dt1, dt2)
        reader.read_historical_data(dt1, dt2)
        data_value = reader.get_data_values()
        self.assertEqual(len(data_value._val), 3)

    def test_observation_to_netcdf_stream(self):
        """Test convert observation value to netcdf stream."""
        dt1 = datetime(2019, 11, 1, 0, 0, 0)
        dt2 = datetime(2019, 11, 2, 0, 0, 0)
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt1,
            value=100
        )
        MeasurementFactory.create(
            station=self.station,
            dataset_attribute=self.dataset_attr,
            date_time=dt2,
            value=200
        )
        reader = ObservationDatasetReader(
            self.dataset, [self.dataset_attr], self.location_input,
            dt1, dt2
        )
        qs = reader.get_measurements(dt1, dt2)
        reader_value = ObservationReaderValue(
            qs, self.location_input, [self.dataset_attr],
            self.start_date, self.end_date, [self.station],
            qs.count()
        )
        d = reader_value.to_netcdf_stream()
        res = list(d)
        self.assertIsNotNone(res)


class TestObservationParquetReader(TestCase):
    """Unit tests for ObservationParquetReader class."""

    def setUp(self):
        """Set up test environment."""
        self.dataset = DatasetFactory.create(
            provider=ProviderFactory(name='Tahmo')
        )
        self.attribute = AttributeFactory.create(
            name='Surface air temperature',
            variable_name='surface_air_temperature'
        )
        self.dataset_attr = DatasetAttributeFactory.create(
            dataset=self.dataset,
            attribute=self.attribute,
            source='surface_air_temperature'
        )

        # Mock S3 settings
        self.s3_mock = {
            "AWS_BUCKET_NAME": "test-bucket",
            "AWS_DIR_PREFIX": "test_prefix/",
            "AWS_ACCESS_KEY_ID": "mock-key",
            "AWS_SECRET_ACCESS_KEY": "mock-secret",
            "AWS_ENDPOINT_URL": "http://localhost:9000"
        }

        self.start_date = datetime(2020, 1, 1)
        self.end_date = datetime(2020, 12, 31)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    def test_initialize_reader(
        self,
        mock_get_directory_path,
        mock_connection
    ):
        """Test initialization of ObservationParquetReader."""
        # Mock the S3 directory path to prevent querying DataSourceFile
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Create a dummy bbox location input
        bbox = [-180, -90, 180, 90]
        location_input = DatasetReaderInput.from_bbox(bbox)

        # Initialize the reader
        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Ensure query is not set before calling read_historical_data
        self.assertFalse(hasattr(reader, "query"))

        # Call read_historical_data (this will use the mocked S3 path)
        reader.read_historical_data(self.start_date, self.end_date)

        # Now assert query exists
        self.assertTrue(hasattr(reader, "query"))
        self.assertIsInstance(reader.query, str)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    def test_generate_query_for_point(
        self,
        mock_get_directory_path,
        mock_connection
    ):
        """Test SQL query generation for POINT location input."""
        self.station = StationFactory.create(
            geometry=Point(26.97, -12.56, srid=4326),
            provider=self.dataset.provider
        )
        # Mock the S3 directory path to prevent querying DataSourceFile
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Create a POINT location input
        location_input = DatasetReaderInput.from_point(Point(36.8219, -1.2921))

        # Initialize the reader
        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Call read_historical_data (this will use the mocked S3 path)
        reader.read_historical_data(self.start_date, self.end_date)

        # Assert query is generated correctly
        self.assertIn("FROM read_parquet(", reader.query)
        self.assertIn("WHERE year >=", reader.query)
        self.assertIn("AND st_code =", reader.query)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    def test_generate_query_for_bbox(
        self,
        mock_get_directory_path,
        mock_connection
    ):
        """Test SQL query generation for BBOX location input."""
        # Mock the S3 path
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        bbox = [-180.0, -90.0, 180.0, 90.0]
        location_input = DatasetReaderInput.from_bbox(bbox)

        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        reader.read_historical_data(self.start_date, self.end_date)

        normalized_query = "".join(reader.query.split())
        expected_substring = "ST_MakeEnvelope(-180.0,-90.0,180.0,90.0)"

        self.assertIn(expected_substring, normalized_query)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    def test_generate_query_for_polygon(
        self,
        mock_get_directory_path,
        mock_connection
    ):
        """Test SQL query generation for POLYGON location input."""
        # Mock the S3 directory path to avoid querying the database
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Create a POLYGON location input
        polygon_wkt = (
            "POLYGON((36.8 -1.3, 37.0 -1.3, 37.0 -1.1, 36.8 -1.1, 36.8 -1.3))"
        )
        location_input = DatasetReaderInput.from_polygon(
            GEOSGeometry(polygon_wkt)
        )

        # Initialize the reader
        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Call read_historical_data (this will use the mocked S3 path)
        reader.read_historical_data(self.start_date, self.end_date)

        # Assert query is generated correctly
        self.assertIn("FROM read_parquet(", reader.query)
        self.assertIn("WHERE year >=", reader.query)
        self.assertIn("ST_Within(ST_GeomFromWKB(geometry),", reader.query)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    def test_generate_query_for_list_of_points(
        self,
        mock_get_directory_path,
        mock_connection
    ):
        """Test SQL query generation for LIST_OF_POINT location input."""
        # Mock the S3 directory path to avoid database dependency
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Ensure stations exist before test
        self.station_1 = StationFactory.create(
            geometry=Point(36.8, -1.3, srid=4326),
            provider=self.dataset.provider
        )
        self.station_2 = StationFactory.create(
            geometry=Point(37.1, -1.2, srid=4326),
            provider=self.dataset.provider
        )
        self.station_3 = StationFactory.create(
            geometry=Point(36.9, -1.4, srid=4326),
            provider=self.dataset.provider
        )

        # Create a LIST_OF_POINT location input
        points = MultiPoint([
            Point(36.8, -1.3),
            Point(37.1, -1.2),
            Point(36.9, -1.4)
        ])
        location_input = DatasetReaderInput.from_list_of_points(points)

        # Initialize the reader
        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Call read_historical_data (this will use the mocked S3 path)
        reader.read_historical_data(self.start_date, self.end_date)

        # Assert query is generated correctly
        self.assertIn("FROM read_parquet(", reader.query)
        self.assertIn("WHERE year >=", reader.query)
        self.assertIn("AND st_code IN (", reader.query)

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    @patch(
        "gap.providers.observation.ObservationParquetReaderValue.to_csv"
    )
    def test_read_historical_data(
        self,
        mock_to_csv,
        mock_get_directory_path,
        mock_get_connection
    ):
        """Test reading historical data calls the CSV export function."""
        # Mock S3 path to avoid querying `DataSourceFile`
        self.station = StationFactory.create(
            geometry=Point(26.97, -12.56, srid=4326),
            provider=self.dataset.provider
        )
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Mock DuckDB connection to prevent real queries
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # Mock `to_csv` to prevent actual file operations
        mock_to_csv.return_value = "s3://test-bucket/tahmo/mock.csv"

        # Create mock dataset & location
        location_input = DatasetReaderInput.from_point(Point(36.8, -1.3))

        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Run the function
        reader.read_historical_data(self.start_date, self.end_date)

        # Ensure `to_csv` was called (CSV export is executed)
        mock_to_csv.assert_called_once()

    @patch("gap.providers.observation.ObservationParquetReaderValue.to_csv")
    def test_csv_export(self, mock_to_csv):
        """Test CSV export is triggered correctly."""
        point = Point(x=26.97, y=-12.56, srid=4326)
        location_input = DatasetReaderInput.from_point(point)

        reader_value = ObservationParquetReaderValue(
            duckdb.connect(),
            location_input,
            [self.dataset_attr],
            self.start_date,
            self.end_date,
            "SELECT * FROM table"
        )

        reader_value.to_csv()

        mock_to_csv.assert_called_once()

    @patch(
        "gap.providers.observation.ObservationParquetReaderValue.to_csv_stream"
    )
    def test_csv_stream_export(self, mock_to_csv_stream):
        """Test CSV stream export is triggered correctly."""
        point = Point(x=26.97, y=-12.56, srid=4326)
        location_input = DatasetReaderInput.from_point(point)

        reader_value = ObservationParquetReaderValue(
            duckdb.connect(),
            location_input,
            [self.dataset_attr],
            self.start_date,
            self.end_date,
            "SELECT * FROM table"
        )

        reader_value.to_csv_stream()

        mock_to_csv_stream.assert_called_once()

    @patch("gap.providers.observation.ObservationParquetReaderValue.to_netcdf")
    def test_netcdf_export(self, mock_to_netcdf):
        """Test NetCDF export is triggered correctly."""
        point = Point(x=26.97, y=-12.56, srid=4326)
        location_input = DatasetReaderInput.from_point(point)

        reader_value = ObservationParquetReaderValue(
            duckdb.connect(),
            location_input,
            [self.dataset_attr],
            self.start_date,
            self.end_date,
            "SELECT * FROM table"
        )

        reader_value.to_netcdf()

        mock_to_netcdf.assert_called_once()

    @patch(
        "gap.providers.observation.ObservationParquetReader._get_connection"
    )
    @patch(
        (
            "gap.providers.observation."
            "ObservationParquetReader._get_directory_path"
        )
    )
    @patch("gap.providers.observation.ObservationParquetReaderValue.to_netcdf")
    def test_read_historical_data_netcdf(
        self,
        mock_to_netcdf,
        mock_get_directory_path,
        mock_get_connection
    ):
        """Test reading historical data calls the NetCDF export function."""
        self.station = StationFactory.create(
            geometry=Point(26.97, -12.56, srid=4326),
            provider=self.dataset.provider
        )
        mock_get_directory_path.return_value = "s3://test-bucket/tahmo/"

        # Mock DuckDB connection to prevent real queries
        mock_conn = MagicMock()
        mock_get_connection.return_value = mock_conn

        # Mock `to_netcdf` to prevent actual file operations
        mock_to_netcdf.return_value = "s3://test-bucket/tahmo/mock.nc"

        # Create mock dataset & location
        location_input = DatasetReaderInput.from_point(Point(36.8, -1.3))

        reader = ObservationParquetReader(
            self.dataset, [self.dataset_attr], location_input,
            self.start_date, self.end_date
        )

        # Run the function
        reader.read_historical_data(self.start_date, self.end_date)
        reader.get_data_values().to_netcdf()

        # Ensure `to_netcdf` was called (NetCDF export is executed)
        mock_to_netcdf.assert_called_once()
