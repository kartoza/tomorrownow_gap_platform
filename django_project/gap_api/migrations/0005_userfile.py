# Generated by Django 4.2.7 on 2024-12-08 15:52

from django.conf import settings
from django.db import migrations, models
import django.db.models.deletion


class Migration(migrations.Migration):

    dependencies = [
        migrations.swappable_dependency(settings.AUTH_USER_MODEL),
        ('gap_api', '0004_apiratelimiter'),
    ]

    operations = [
        migrations.CreateModel(
            name='UserFile',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('name', models.CharField(help_text='File path to the storage.', max_length=512)),
                ('created_on', models.DateTimeField(auto_now_add=True)),
                ('query_params', models.JSONField(default=dict, help_text='Query parameters that generate the file.')),
                ('query_hash', models.CharField(blank=True, editable=False, help_text='Hash that can be used to cache the query result.', max_length=512)),
                ('size', models.IntegerField(default=0)),
                ('user', models.ForeignKey(on_delete=django.db.models.deletion.CASCADE, to=settings.AUTH_USER_MODEL)),
            ],
        ),
    ]
