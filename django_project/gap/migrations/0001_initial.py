# Generated by Django 4.2.7 on 2024-07-01 03:50

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
            name='Measurement',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('unit', models.CharField(blank=True, max_length=512, null=True)),
                ('time', models.DateTimeField()),
                ('value', models.FloatField()),
                ('attribute', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.attribute')),
                ('station', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.station')),
            ],
            options={
                'unique_together': {('station', 'attribute', 'time')},
            },
        ),
    ]
