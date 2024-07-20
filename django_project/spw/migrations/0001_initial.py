# Generated by Django 4.2.7 on 2024-07-20 12:39

from django.conf import settings
import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion
import spw.models


class Migration(migrations.Migration):

    initial = True

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
    ]

    operations = [
        migrations.CreateModel(
            name='RModel',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=256)),
                ('version', models.FloatField()),
                ('code', models.TextField()),
                ('notes', models.TextField(blank=True, null=True)),
                ('created_on', models.DateTimeField()),
                ('updated_on', models.DateTimeField()),
                ('created_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
                ('updated_by', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, related_name='rmodel_updater', to=settings.AUTH_USER_MODEL)),
            ],
        ),
        migrations.CreateModel(
            name='RModelOutput',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('type', models.CharField(choices=[('goNoGo', 'goNoGo'), ('days_h2to_f2', 'days_h2to_f2'), ('days_f3to_f5', 'days_f3to_f5'), ('days_f6to_f13', 'days_f6to_f13'), ('nearDaysLTNPercent', 'nearDaysLTNPercent'), ('nearDaysCurPercent', 'nearDaysCurPercent')], max_length=100)),
                ('variable_name', models.CharField(max_length=100)),
                ('model', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='spw.rmodel')),
            ],
        ),
        migrations.CreateModel(
            name='RModelExecutionLog',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('location_input', django.contrib.gis.db.models.fields.GeometryField(blank=True, null=True, srid=4326)),
                ('input_file', models.FileField(blank=True, null=True, upload_to=spw.models.r_model_input_file_path)),
                ('output', models.JSONField(blank=True, default=dict, null=True)),
                ('start_date_time', models.DateTimeField(blank=True, null=True)),
                ('end_date_time', models.DateTimeField(blank=True, null=True)),
                ('status', models.CharField(choices=[('RUNNING', 'RUNNING'), ('SUCCESS', 'SUCCESS'), ('FAILED', 'FAILED')], default='RUNNING', max_length=512)),
                ('errors', models.TextField(blank=True, null=True)),
                ('model', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='spw.rmodel')),
            ],
        ),
    ]
