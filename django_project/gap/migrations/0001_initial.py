# Generated by Django 4.2.7 on 2024-07-05 15:16

import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    initial = True

    dependencies = [
    ]

    operations = [
        migrations.CreateModel(
            name='Attribute',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('variable_name', models.CharField(max_length=512)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Country',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('iso_a3', models.CharField(max_length=255, unique=True)),
                ('geometry', django.contrib.gis.db.models.fields.MultiPolygonField(blank=True, null=True, srid=4326)),
            ],
            options={
                'verbose_name_plural': 'countries',
                'ordering': ['name'],
            },
        ),
        migrations.CreateModel(
            name='Dataset',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('type', models.CharField(choices=[('Climate Reanalysis', 'Climate Reanalysis'), ('Short-term Forecast', 'Short-term Forecast'), ('Seasonal Forecast', 'Seasonal Forecast'), ('Ground Observational', 'Ground Observational'), ('Airborne Observational', 'Airborne Observational')], max_length=512)),
                ('time_step', models.CharField(choices=[('DAILY', 'DAILY'), ('HOURLY', 'HOURLY')], max_length=512)),
                ('store_type', models.CharField(choices=[('TABLE', 'TABLE'), ('NETCDF', 'NETCDF'), ('EXT_API', 'EXT_API')], max_length=512)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='IngestorSession',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ingestor_type', models.CharField(choices=[('Tahmo', 'Tahmo')], default='Tahmo', max_length=512)),
                ('file', models.FileField(blank=True, null=True, upload_to='ingestors/')),
                ('status', models.CharField(choices=[('RUNNING', 'RUNNING'), ('SUCCESS', 'SUCCESS'), ('FAILED', 'FAILED')], default='RUNNING', max_length=512)),
                ('notes', models.TextField(blank=True, null=True)),
                ('run_at', models.DateTimeField(auto_now_add=True)),
                ('end_at', models.DateTimeField(blank=True, null=True)),
            ],
        ),
        migrations.CreateModel(
            name='ObservationType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Provider',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Unit',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='Station',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('code', models.CharField(max_length=512)),
                ('geometry', django.contrib.gis.db.models.fields.PointField(srid=4326)),
                ('country', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.country')),
                ('observation_type', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.observationtype')),
                ('provider', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.provider')),
            ],
            options={
                'unique_together': {('code', 'provider')},
            },
        ),
        migrations.CreateModel(
            name='NetCDFFile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='Filename with its path in the object storage (S3)', max_length=512)),
                ('start_date_time', models.DateTimeField()),
                ('end_date_time', models.DateTimeField()),
                ('created_on', models.DateTimeField()),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.dataset')),
            ],
        ),
        migrations.CreateModel(
            name='DatasetAttribute',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source', models.CharField(help_text='Variable name in the source', max_length=512)),
                ('attribute', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.attribute')),
                ('dataset', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.dataset')),
                ('source_unit', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.unit')),
            ],
        ),
        migrations.AddField(
            model_name='dataset',
            name='provider',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.provider'),
        ),
        migrations.AddField(
            model_name='attribute',
            name='unit',
            field=models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.unit'),
        ),
        migrations.CreateModel(
            name='Measurement',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('date_time', models.DateTimeField()),
                ('value', models.FloatField()),
                ('dataset_attribute', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.datasetattribute')),
                ('station', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.station')),
            ],
            options={
                'unique_together': {('station', 'dataset_attribute', 'date_time')},
            },
        ),
    ]
