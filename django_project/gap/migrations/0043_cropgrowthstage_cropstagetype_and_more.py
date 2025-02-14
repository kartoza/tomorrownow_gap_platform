# Generated by Django 4.2.7 on 2024-12-20 11:05

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0042_alter_datasourcefilecache_size'),
    ]

    operations = [
        migrations.CreateModel(
            name='CropGrowthStage',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
            ],
            options={
                'ordering': ['name'],
                'abstract': False,
            },
        ),
        migrations.CreateModel(
            name='CropStageType',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(max_length=512)),
                ('description', models.TextField(blank=True, null=True)),
                ('alias', models.CharField(blank=True, help_text='Name Alias', max_length=50, null=True)),
            ],
            options={
                'ordering': ['name'],
                'abstract': False,
            },
        ),
        migrations.AlterField(
            model_name='collectorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API'), ('Cabi Prise Excel', 'Cabi Prise Excel'), ('CBAM Bias Adjusted', 'CBAM Bias Adjusted'), ('DCAS Rules', 'DCAS Rules')], default='Tahmo', max_length=512),
        ),
        migrations.AlterField(
            model_name='ingestorsession',
            name='ingestor_type',
            field=models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io'), ('Arable', 'Arable'), ('Grid', 'Grid'), ('Tahmo API', 'Tahmo API'), ('Tio Forecast Collector', 'Tio Forecast Collector'), ('WindBorne Systems API', 'WindBorne Systems API'), ('Cabi Prise Excel', 'Cabi Prise Excel'), ('CBAM Bias Adjusted', 'CBAM Bias Adjusted'), ('DCAS Rules', 'DCAS Rules')], default='Tahmo', max_length=512),
        ),
    ]
