# Generated by Django 4.2.7 on 2024-10-02 05:18

import django.contrib.gis.db.models.fields
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0025_farmsuitableplantingwindowsignal_last_2_days_and_more'),
    ]

    operations = [
        migrations.AddField(
            model_name='station',
            name='altitude',
            field=models.FloatField(blank=True, help_text='Altitude in meters', null=True),
        ),
        migrations.CreateModel(
            name='StationHistory',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('geometry', django.contrib.gis.db.models.fields.PointField(srid=4326)),
                ('altitude', models.FloatField(blank=True, help_text='Altitude in meters', null=True)),
                ('date_time', models.DateTimeField()),
                ('station', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to='gap.station')),
            ],
            options={
                'unique_together': {('station', 'date_time')},
            },
        ),
    ]
