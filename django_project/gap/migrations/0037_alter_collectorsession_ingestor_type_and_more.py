# Generated by Django 4.2.7 on 2024-11-06 08:14

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0036_pest_short_name'),
    ]

    operations = [
        migrations.AlterField(
            model_name='collectorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API'), ('Cabi Prise Excel', 'Cabi Prise Excel'), ('CBAM Bias Adjusted', 'CBAM Bias Adjusted')], default='Tahmo', max_length=512),
        ),
        migrations.AlterField(
            model_name='ingestorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API'), ('Cabi Prise Excel', 'Cabi Prise Excel'), ('CBAM Bias Adjusted', 'CBAM Bias Adjusted')], default='Tahmo', max_length=512),
        ),
    ]
