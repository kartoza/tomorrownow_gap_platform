# Generated by Django 4.2.7 on 2024-08-27 02:35

from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        ('gap', '0012_attribute_is_active'),
    ]

    operations = [
        migrations.AlterField(
            model_name='datasourcefile',
            name='format',
            field=models.CharField(choices=[('NETCDF', 'NETCDF'), ('ZARR', 'ZARR'), ('ZIP_FILE', 'ZIP_FILE')], max_length=512),
        ),
        migrations.CreateModel(
            name='CollectorSession',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('ingestor_type', models.CharField(choices=[('Tahmo', 'Tahmo'), ('Farm', 'Farm'), ('CBAM', 'CBAM'), ('Salient', 'Salient'), ('Tomorrow.io', 'Tomorrow.io')], default='Tahmo', max_length=512)),
                ('status', models.CharField(choices=[('RUNNING', 'RUNNING'), ('SUCCESS', 'SUCCESS'), ('FAILED', 'FAILED'), ('CANCELLED', 'CANCELLED')], default='RUNNING', max_length=512)),
                ('notes', models.TextField(blank=True, null=True)),
                ('run_at', models.DateTimeField(auto_now_add=True)),
                ('end_at', models.DateTimeField(blank=True, null=True)),
                ('additional_config', models.JSONField(blank=True, default=dict, null=True)),
                ('is_cancelled', models.BooleanField(default=False)),
                ('dataset_files', models.ManyToManyField(to='gap.datasourcefile')),
            ],
            options={
                'abstract': False,
            },
        ),
        migrations.AddField(
            model_name='ingestorsession',
            name='collector',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.SET_NULL, to='gap.collectorsession'),
        ),
    ]
