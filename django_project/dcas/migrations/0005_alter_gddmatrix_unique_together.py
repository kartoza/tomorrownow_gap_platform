# Generated by Django 4.2.7 on 2025-01-14 12:49

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('dcas', '0004_dcasrequest_dcasoutput_dcaserrorlog'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='gddmatrix',
            unique_together={('crop', 'crop_stage_type', 'config', 'crop_growth_stage', 'gdd_threshold')},
        ),
    ]
